package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max

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

    private val dbExecutor = Executors.newFixedThreadPool(
        1000,
        NamedThreadFactory("payment-db-executor")
    ) as ThreadPoolExecutor

    private val ratePerSecond = properties.rateLimitPerSec.toLong().coerceAtLeast(1L)
    private val intervalNanos = 1_000_000_000L / ratePerSecond
    private val nextAllowedTimeNanos = AtomicLong(0L)

    private val rateLimiterExecutor = Executors.newScheduledThreadPool(
        2, NamedThreadFactory("payment-rl-$accountName")
    ) as ScheduledThreadPoolExecutor

    private val parallelSemaphore = java.util.concurrent.Semaphore(properties.parallelRequests)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = properties.parallelRequests * 2
            maxRequestsPerHost = properties.parallelRequests * 2
        })
        .connectionPool(ConnectionPool(200, 5, TimeUnit.MINUTES))
        .connectTimeout(java.time.Duration.ofSeconds(5))
        .readTimeout(java.time.Duration.ofSeconds(70))
        .writeTimeout(java.time.Duration.ofSeconds(5))
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
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, paymentStartedAt, java.time.Duration.ofMillis(enterTime - paymentStartedAt))
            }
        }, dbExecutor)

        val proceed = {
            try {
                parallelSemaphore.acquire()

                val url = "http://$paymentProviderHostPort/external/process?" +
                        "serviceName=${properties.serviceName}&token=$token&accountName=$accountName&" +
                        "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

                val request = Request.Builder().url(url).post(emptyBody).build()

                client.newCall(request).enqueue(object : Callback {
                    override fun onFailure(call: Call, e: IOException) {
                        try {
                            val reason = if (e is SocketTimeoutException) "timeout" else e.message ?: "io_error"
                            logger.warn("[$accountName] FAILED $paymentId tx=$transactionId: $reason")

                            CompletableFuture.runAsync({
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, System.currentTimeMillis(), transactionId, reason)
                                }
                            }, dbExecutor)

                            result.complete(false)
                        } finally {
                            parallelSemaphore.release()
                        }
                    }

                    override fun onResponse(call: Call, response: Response) {
                        try {
                            val bodyText = response.body?.string().orEmpty()
                            val extResp = try {
                                mapper.readValue(bodyText, ExternalSysResponse::class.java)
                            } catch (ex: Exception) {
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, ex.message)
                            }

                            CompletableFuture.runAsync({
                                paymentESService.update(paymentId) {
                                    it.logProcessing(extResp.result, System.currentTimeMillis(), transactionId, extResp.message)
                                }
                            }, dbExecutor)

                            result.complete(extResp.result)
                        } catch (t: Throwable) {
                            result.complete(false)
                        } finally {
                            response.close()
                            parallelSemaphore.release()
                        }
                    }
                })

            } catch (ex: Exception) {
                parallelSemaphore.release()
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