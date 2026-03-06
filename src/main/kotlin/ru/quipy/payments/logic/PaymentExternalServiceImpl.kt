package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Semaphore
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val mapper = ObjectMapper().registerKotlinModule()

        private const val HEDGE1_DELAY_MS: Long = 180
        private const val HEDGE2_DELAY_MS: Long = 360

        private val REQUEST_TIMEOUT: Duration = Duration.ofMillis(1500)
        private const val UPDATE_RETRY_ATTEMPTS = 5
    }

    private val accountName = properties.accountName
    private val serviceName = properties.serviceName

    private val scheduler: ScheduledExecutorService =
        Executors.newScheduledThreadPool(4, NamedThreadFactory("payment-hedge-$accountName"))

    private val dbExecutor = Executors.newFixedThreadPool(
        min(16, max(4, Runtime.getRuntime().availableProcessors() * 2)),
        NamedThreadFactory("payment-db-$accountName")
    ) as ThreadPoolExecutor

    private val semaphore = Semaphore(properties.parallelRequests)

    private val rateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong().coerceAtLeast(1),
        window = Duration.ofSeconds(1)
    )

    private val httpExecutor = Executors.newFixedThreadPool(
        min(max(64, properties.parallelRequests), 512),
        NamedThreadFactory("payment-http-$accountName")
    )

    private val http2Client: HttpClient =
        HttpClient.newBuilder()
            .executor(httpExecutor)
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(5))
            .build()

    private val externalReqCounters = ConcurrentHashMap<String, Counter>()

    private fun incReq(kind: String, outcome: String) {
        val key = "$kind|$outcome"
        externalReqCounters.computeIfAbsent(key) {
            Counter.builder("payment_external_requests_total")
                .tag("account", accountName)
                .tag("kind", kind)
                .tag("outcome", outcome)
                .register(meterRegistry)
        }.increment()
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {

        val result = CompletableFuture<Boolean>()
        val completed = AtomicBoolean(false)

        val nowMs = now()
        val remainingMs = max(0L, deadline - nowMs)

        val txId = UUID.randomUUID()
        val idempotencyKey = txId.toString()

        runDbAsync {
            updateWithRetry(paymentId) { state ->
                state.logSubmission(
                    success = true,
                    transactionId = txId,
                    startedAt = paymentStartedAt,
                    spentInQueueDuration = Duration.ofMillis(now() - paymentStartedAt)
                )
            }
        }

        if (remainingMs <= 0) {
            incReq("primary", "deadline_exceeded")
            runDbAsync {
                updateWithRetry(paymentId) { state ->
                    state.logProcessing(
                        success = false,
                        processedAt = now(),
                        transactionId = txId,
                        reason = "deadline_exceeded"
                    )
                }
            }
            result.complete(false)
            return result
        }

        if (!rateLimiter.tickBlocking(Duration.ofMillis(remainingMs))) {
            incReq("primary", "rate_limited")
            runDbAsync {
                updateWithRetry(paymentId) { state ->
                    state.logProcessing(false, now(), txId, reason = "Rate limit exceed")
                }
            }
            result.complete(false)
            return result
        }

        val acquired = try {
            semaphore.tryAcquire(remainingMs, TimeUnit.MILLISECONDS)
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            false
        }

        if (!acquired) {
            incReq("primary", "semaphore_timeout")
            runDbAsync {
                updateWithRetry(paymentId) { state ->
                    state.logProcessing(false, now(), txId, reason = "Semaphore timeout")
                }
            }
            result.complete(false)
            return result
        }

        result.whenComplete { _, _ -> semaphore.release() }

        fun buildRequest(): HttpRequest {
            val url =
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName&token=$token&accountName=$accountName" +
                        "&transactionId=$txId&paymentId=$paymentId&amount=$amount"

            return HttpRequest.newBuilder()
                .uri(URI(url))
                .header("x-idempotency-key", idempotencyKey)
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.noBody())
                .build()
        }

        val primary = sendAsync("primary", buildRequest())
        val hedge1 = CompletableFuture<HttpResponse<String>>()
        val hedge2 = CompletableFuture<HttpResponse<String>>()

        val all: Array<CompletableFuture<*>> = arrayOf(primary, hedge1, hedge2)

        fun maybeFinishFalseIfAllDone(force: Boolean = false) {
            if (completed.get()) return
            val allDone = force || all.all { it.isDone || it.isCancelled }
            if (!allDone) return

            if (completed.compareAndSet(false, true)) {
                result.complete(false)
            }
        }

        fun attachAttempt(kind: String, f: CompletableFuture<HttpResponse<String>>) {
            f.whenComplete { resp, ex ->
                if (completed.get()) return@whenComplete

                if (f.isCancelled) {
                    incReq(kind, "canceled")
                    maybeFinishFalseIfAllDone()
                    return@whenComplete
                }

                if (ex != null) {
                    val cause = (ex as? CompletionException)?.cause ?: ex
                    val outcome = if (cause is SocketTimeoutException) "timeout" else "fail"
                    incReq(kind, outcome)

                    runDbAsync {
                        updateWithRetry(paymentId) { state ->
                            state.logProcessing(false, now(), txId, reason = cause.message ?: outcome)
                        }
                    }

                    maybeFinishFalseIfAllDone()
                    return@whenComplete
                }

                if (resp == null) {
                    incReq(kind, "fail")
                    runDbAsync {
                        updateWithRetry(paymentId) { state ->
                            state.logProcessing(false, now(), txId, reason = "empty_response")
                        }
                    }
                    maybeFinishFalseIfAllDone()
                    return@whenComplete
                }

                val body = try {
                    mapper.readValue(resp.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    ExternalSysResponse(txId.toString(), paymentId.toString(), false, e.message)
                }

                runDbAsync {
                    updateWithRetry(paymentId) { state ->
                        state.logProcessing(body.result, now(), txId, reason = body.message)
                    }
                }

                if (body.result) {
                    incReq(kind, "success")
                    if (completed.compareAndSet(false, true)) {
                        all.forEach { other ->
                            if (other !== f && !other.isDone) other.cancel(true)
                        }
                        result.complete(true)
                    }
                } else {
                    incReq(kind, "fail")
                    if (now() > deadline) {
                        maybeFinishFalseIfAllDone(force = true)
                    } else {
                        maybeFinishFalseIfAllDone()
                    }
                }
            }
        }

        attachAttempt("primary", primary)
        attachAttempt("hedge", hedge1)
        attachAttempt("hedge2", hedge2)
        scheduler.schedule({
            if (completed.get() || now() > deadline || primary.isDone) {
                if (!hedge1.isDone) hedge1.cancel(true)
                return@schedule
            }
            completeWith(hedge1, sendAsync("hedge", buildRequest()))
        }, HEDGE1_DELAY_MS, TimeUnit.MILLISECONDS)

        scheduler.schedule({
            if (completed.get() || now() > deadline || primary.isDone || hedge1.isDone) {
                if (!hedge2.isDone) hedge2.cancel(true)
                return@schedule
            }
            completeWith(hedge2, sendAsync("hedge2", buildRequest()))
        }, HEDGE2_DELAY_MS, TimeUnit.MILLISECONDS)

        return result
    }

    private fun sendAsync(kind: String, req: HttpRequest): CompletableFuture<HttpResponse<String>> {
        incReq(kind, "sent")
        return http2Client.sendAsync(req, HttpResponse.BodyHandlers.ofString())
    }

    private fun completeWith(
        target: CompletableFuture<HttpResponse<String>>,
        actual: CompletableFuture<HttpResponse<String>>
    ) {
        actual.whenComplete { resp, ex ->
            if (target.isDone) return@whenComplete
            if (ex != null) target.completeExceptionally(ex)
            else target.complete(resp)
        }
    }

    private fun runDbAsync(block: () -> Unit) {
        CompletableFuture.runAsync(block, dbExecutor)
    }

    private fun updateWithRetry(
        paymentId: UUID,
        attempts: Int = UPDATE_RETRY_ATTEMPTS,
        block: (PaymentAggregateState) -> Event<PaymentAggregate>
    ) {
        var last: Exception? = null
        for (i in 1..attempts) {
            try {
                paymentESService.update(paymentId) { state -> block(state) }
                return
            } catch (e: Exception) {
                last = e
                val backoffMs = (5L * i) + Random.nextLong(0, 15)
                try {
                    Thread.sleep(backoffMs)
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw e
                }
            }
        }
        throw last ?: IllegalStateException("updateWithRetry failed without exception")
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private fun now(): Long = System.currentTimeMillis()