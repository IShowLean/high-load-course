package ru.quipy.payments.logic

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
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
        private val mapper = ObjectMapper().registerKotlinModule()
        private val emptyBody = ByteArray(0).toRequestBody(null)
    }

    private val accountName = properties.accountName
    private val timeOut = Duration.ofSeconds(0)

    private val clients: List<OkHttpClient> = List(15) {
        OkHttpClient.Builder()
            .dispatcher(Dispatcher(Executors.newFixedThreadPool(250)).apply {
                maxRequests = 250
                maxRequestsPerHost = 250
            })
            .connectionPool(ConnectionPool(5, 5, TimeUnit.MINUTES))
            .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
            .readTimeout(90, TimeUnit.SECONDS)
            .build()
    }

    private val clientIndex = AtomicInteger(0)
    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(properties.parallelRequests)  // блокирующий!

    override fun performPaymentAsync(
        paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long
    ): CompletableFuture<Boolean> {

        val transactionId = UUID.randomUUID()
        val cf = CompletableFuture<Boolean>()

        paymentESService.update(paymentId) {
            it.logSubmission(true, transactionId, System.currentTimeMillis(), Duration.ofMillis(System.currentTimeMillis() - paymentStartedAt))
        }

        try {
            ongoingWindow.acquire()
            rateLimiter.tickBlocking()

            val url = if (timeOut.isZero) {
                "https://$paymentProviderHostPort/external/process?serviceName=${properties.serviceName}&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            } else {
                "https://$paymentProviderHostPort/external/process?timeout=$timeOut&serviceName=${properties.serviceName}&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"
            }

            val request = Request.Builder()
                .url(url)
                .post(emptyBody)
                .build()

            val client = clients[clientIndex.getAndIncrement() % 15]

            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, System.currentTimeMillis(), transactionId, e.message ?: "IO Error")
                    }
                    ongoingWindow.release()
                    cf.complete(false)
                }

                override fun onResponse(call: Call, response: Response) {
                    response.use {
                        val bodyStr = response.body?.string() ?: ""
                        val success = try {
                            val resp = mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                            paymentESService.update(paymentId) {
                                it.logProcessing(resp.result, System.currentTimeMillis(), transactionId, resp.message)
                            }
                            resp.result
                        } catch (ex: Exception) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, System.currentTimeMillis(), transactionId, "Parse error")
                            }
                            false
                        }
                        ongoingWindow.release()
                        cf.complete(success)
                    }
                }
            })

        } catch (e: Exception) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, System.currentTimeMillis(), transactionId, "Rate limit exceeded")
            }
            ongoingWindow.release()
            cf.complete(false)
        }

        return cf
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}