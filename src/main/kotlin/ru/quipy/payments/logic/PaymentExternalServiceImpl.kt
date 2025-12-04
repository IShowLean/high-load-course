package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
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
        private val emptyBody = RequestBody.create(null, ByteArray(0))
        private val mapper = ObjectMapper().registerKotlinModule()

        private fun createRateLimiter(ratePerSec: Int): RateLimiter {
            val config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(ratePerSec)
                .timeoutDuration(Duration.ofHours(1))
                .build()
            return RateLimiterRegistry.of(config).rateLimiter("rl-${UUID.randomUUID()}")
        }
    }

    private val accountName = properties.accountName
    private val rateLimiter = createRateLimiter(properties.rateLimitPerSec)
    private val ongoingWindow = java.util.concurrent.Semaphore(properties.parallelRequests)

    private val clients: List<OkHttpClient> = List(30) {
        val maxPerClient = max(200, properties.parallelRequests * 3 / 2 / 30 + 20)
        val dispatcher = Dispatcher().apply {
            maxRequests = maxPerClient * 2
            maxRequestsPerHost = maxPerClient * 2
        }

        OkHttpClient.Builder()
            .dispatcher(dispatcher)
            .connectionPool(ConnectionPool(200, 5, TimeUnit.MINUTES))
            .connectTimeout(Duration.ofSeconds(5))
            .readTimeout(Duration.ofSeconds(70))
            .writeTimeout(Duration.ofSeconds(5))
            .retryOnConnectionFailure(true)
            .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .build()
    }

    private val clientIndex = java.util.concurrent.atomic.AtomicInteger(0)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {

        val transactionId = UUID.randomUUID()
        val queueEnterTime = System.currentTimeMillis()
        val resultFuture = CompletableFuture<Boolean>()

        paymentESService.update(paymentId) {
            it.logSubmission(
                success = true,
                transactionId = transactionId,
                startedAt = paymentStartedAt,
                spentInQueueDuration = Duration.ofMillis(queueEnterTime - paymentStartedAt)
            )
        }

        try {
            rateLimiter.acquirePermission()
            ongoingWindow.acquire()

            val url = "http://$paymentProviderHostPort/external/process?" +
                    "serviceName=${properties.serviceName}&token=$token&accountName=$accountName&" +
                    "transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

            val request = Request.Builder()
                .url(url)
                .post(emptyBody)
                .build()

            val client = clients[clientIndex.getAndIncrement() and Int.MAX_VALUE % clients.size]

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    val reason = if (e is SocketTimeoutException) "timeout" else e.message ?: "network error"
                    logger.warn("[$accountName] Request failed $paymentId tx=$transactionId: $reason")

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = reason)
                    }

                    resultFuture.complete(false)
                    ongoingWindow.release()
                }

                override fun onResponse(call: Call, response: Response) {
                    try {
                        val bodyText = response.body?.string().orEmpty()
                        val extResponse = try {
                            mapper.readValue(bodyText, ExternalSysResponse::class.java)
                        } catch (ex: Exception) {
                            logger.error("[$accountName] Parse error $paymentId: $bodyText", ex)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, ex.message)
                        }

                        paymentESService.update(paymentId) {
                            it.logProcessing(
                                success = extResponse.result,
                                processedAt = System.currentTimeMillis(),
                                transactionId = transactionId,
                                reason = extResponse.message
                            )
                        }

                        resultFuture.complete(extResponse.result)
                    } catch (t: Throwable) {
                        logger.error("[$accountName] Exception in onResponse $paymentId", t)
                        resultFuture.complete(false)
                    } finally {
                        response.close()
                        ongoingWindow.release()
                    }
                }
            })

        } catch (ex: Exception) {
            ongoingWindow.release()
            logger.error("[$accountName] Failed before sending request $paymentId", ex)
            resultFuture.complete(false)
        }

        return resultFuture
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}