package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        private val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapterImpl::class.java)
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName

    private val dbExecutor = object : ScheduledThreadPoolExecutor(
        200,
        NamedThreadFactory("payment-db-executor")
    ) {
        init {
            maximumPoolSize = 200
            removeOnCancelPolicy = true
            rejectedExecutionHandler = CallerBlockingRejectedExecutionHandler(Duration.ofMinutes(30))
        }
    }.apply {
        io.micrometer.core.instrument.Gauge.builder("db.threadpool.active", this) { it.activeCount.toDouble() }
            .description("Active threads in DB pool ($accountName)")
            .tag("account", accountName)
            .register(meterRegistry)

        io.micrometer.core.instrument.Gauge.builder("db.threadpool.size", this) { it.poolSize.toDouble() }
            .description("Current pool size ($accountName)")
            .tag("account", accountName)
            .register(meterRegistry)

        io.micrometer.core.instrument.Gauge.builder("db.threadpool.queue", this) { it.queue.size.toDouble() }
            .description("Tasks in queue ($accountName)")
            .tag("account", accountName)
            .register(meterRegistry)
    }

    private val rateLimiter: RateLimiter = RateLimiterRegistry.of(
        RateLimiterConfig.custom()
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .limitForPeriod(properties.rateLimitPerSec)
            .timeoutDuration(Duration.ofHours(1))
            .build()
    ).rateLimiter("rl-$accountName")

    private val parallelSemaphore = java.util.concurrent.Semaphore(properties.parallelRequests)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = properties.parallelRequests * 2
            maxRequestsPerHost = properties.parallelRequests * 2
        })
        .connectionPool(ConnectionPool(200, 5, TimeUnit.MINUTES))
        .connectTimeout(Duration.ofSeconds(5))
        .readTimeout(Duration.ofSeconds(70))
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
            paymentESService.update(paymentId) {
                it.logSubmission(true, transactionId, paymentStartedAt, Duration.ofMillis(enterTime - paymentStartedAt))
            }
        }, dbExecutor)

        try {
            rateLimiter.acquirePermission()
            parallelSemaphore.acquire()

            val url = "http://$paymentProviderHostPort/external/process?" +
                    "serviceName=${properties.serviceName}&token=$token&accountName=$accountName&" +
                    "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            val request = Request.Builder().url(url).post(emptyBody).build()

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    val reason = if (e is SocketTimeoutException) "timeout" else e.message ?: "io_error"
                    logger.warn("[$accountName] FAILED $paymentId tx=$transactionId: $reason")

                    CompletableFuture.runAsync({
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, System.currentTimeMillis(), transactionId, reason)
                        }
                    }, dbExecutor)

                    result.complete(false)
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
                    }
                }

                init {
                    parallelSemaphore.release()
                }
            })

        } catch (ex: Exception) {
            parallelSemaphore.release()
            result.complete(false)
        }

        return result
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}