package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.domain.Event
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = ByteArray(0).toRequestBody(null)
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName

    private val locks = java.util.concurrent.ConcurrentHashMap<UUID, java.util.concurrent.locks.ReentrantLock>()

    private fun <T> withPaymentLock(paymentId: UUID, block: () -> T): T {
        val lock = locks.computeIfAbsent(paymentId) { java.util.concurrent.locks.ReentrantLock() }
        lock.lock()
        return try {
            block()
        } finally {
            lock.unlock()
            if (!lock.hasQueuedThreads()) {
                locks.remove(paymentId, lock)
            }
        }
    }

    private fun updateWithRetry(paymentId: UUID, attempts: Int = 5, block: (PaymentAggregateState) -> Any) {
        var last: Exception? = null
        for (i in 1..attempts) {
            try {
                paymentESService.update(paymentId) { state -> block(state) as Event<PaymentAggregate> }
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

    private val dbExecutor = Executors.newFixedThreadPool(
        min(16, max(4, Runtime.getRuntime().availableProcessors() * 2)),
        NamedThreadFactory("payment-db-executor")
    ) as ThreadPoolExecutor

    private val maxParallel = min(properties.parallelRequests, 200)

    private val ratePerSecond = properties.rateLimitPerSec.toLong().coerceAtLeast(1L)
    private val intervalNanos = 1_000_000_000L / ratePerSecond
    private val nextAllowedTimeNanos = AtomicLong(0L)

    private val rateLimiterExecutor = Executors.newScheduledThreadPool(
        2, NamedThreadFactory("payment-rl-$accountName")
    ) as ScheduledThreadPoolExecutor

    private val parallelSemaphore = java.util.concurrent.Semaphore(maxParallel)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = maxParallel
            maxRequestsPerHost = maxParallel
        })
        .connectionPool(ConnectionPool(200, 5, TimeUnit.MINUTES))
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ofSeconds(20))
        .writeTimeout(Duration.ofSeconds(5))
        .retryOnConnectionFailure(true)
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .build()

    init {
        io.micrometer.core.instrument.Gauge.builder("tcp.connections.total", client.connectionPool::connectionCount)
            .description("Total TCP connections ($accountName)")
            .tag("account", accountName)
            .register(meterRegistry)

        io.micrometer.core.instrument.Gauge.builder("tcp.connections.idle", client.connectionPool::idleConnectionCount)
            .description("Idle TCP connections ($accountName)")
            .tag("account", accountName)
            .register(meterRegistry)
    }

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        val transactionId = UUID.randomUUID()
        val enterTime = System.currentTimeMillis()
        val result = CompletableFuture<Boolean>()

        CompletableFuture.runAsync({
            withPaymentLock(paymentId) {
                updateWithRetry(paymentId) { state ->
                    state.logSubmission(
                        success = true,
                        transactionId = transactionId,
                        startedAt = paymentStartedAt,
                        spentInQueueDuration = Duration.ofMillis(enterTime - paymentStartedAt)
                    )
                }
            }
        }, dbExecutor)

        val proceed = {
            var acquired = false
            try {
                parallelSemaphore.acquire()
                acquired = true

                val url = "http://$paymentProviderHostPort/external/process?" +
                        "serviceName=${properties.serviceName}&token=$token&accountName=$accountName&" +
                        "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

                val request = Request.Builder().url(url).post(emptyBody).build()

                client.newCall(request).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        val reason = if (e is SocketTimeoutException) "timeout" else (e.message ?: "io_error")
                        val now = System.currentTimeMillis()
                        logger.warn("[$accountName] FAILED $paymentId tx=$transactionId: $reason")

                        CompletableFuture.runAsync({
                            withPaymentLock(paymentId) {
                                updateWithRetry(paymentId) { state ->
                                    state.logProcessing(
                                        success = false,
                                        processedAt = now,
                                        transactionId = transactionId,
                                        reason = reason
                                    )
                                }
                            }
                        }, dbExecutor)

                        result.complete(false)
                        if (acquired) parallelSemaphore.release()
                    }

                    override fun onResponse(call: Call, response: Response) {
                        try {
                            val bodyText = response.body?.string().orEmpty()
                            val extResp = try {
                                mapper.readValue(bodyText, ExternalSysResponse::class.java)
                            } catch (ex: Exception) {
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, ex.message)
                            }

                            val now = System.currentTimeMillis()

                            CompletableFuture.runAsync({
                                withPaymentLock(paymentId) {
                                    updateWithRetry(paymentId) { state ->
                                        state.logProcessing(
                                            success = extResp.result,
                                            processedAt = now,
                                            transactionId = transactionId,
                                            reason = extResp.message
                                        )
                                    }
                                }
                            }, dbExecutor)

                            result.complete(extResp.result)
                        } catch (_: Throwable) {
                            result.complete(false)
                        } finally {
                            response.close()
                            if (acquired) parallelSemaphore.release()
                        }
                    }
                })
            } catch (_: Exception) {
                if (acquired) parallelSemaphore.release()
                result.complete(false)
            }
        }

        while (true) {
            val current = System.nanoTime()
            val previous = nextAllowedTimeNanos.get()
            val candidate = max(previous + intervalNanos, current)

            if (nextAllowedTimeNanos.compareAndSet(previous, candidate)) {
                val delayNanos = candidate - current
                if (delayNanos <= 0L) {
                    proceed()
                } else {
                    rateLimiterExecutor.schedule(proceed, delayNanos, TimeUnit.NANOSECONDS)
                }
                break
            }
        }

        return result
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}