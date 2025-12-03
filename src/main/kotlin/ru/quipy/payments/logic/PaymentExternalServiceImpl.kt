package ru.quipy.payments.logic

import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import okio.ByteString
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
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
        private val mapper = ObjectMapper().registerKotlinModule()
        private val emptyBody = ByteArray(0).toRequestBody(null)
    }

    private val accountName = properties.accountName

    private val incomingRequestsCounter = Counter.builder("incoming.requests")
        .tag("account", accountName).register(meterRegistry)
    private val incomingFinishedRequestsCounter = Counter.builder("incoming.finished.requests")
        .tag("account", accountName).register(meterRegistry)
    private val outgoingRequestsCounter = Counter.builder("outgoing.requests")
        .tag("account", accountName).register(meterRegistry)
    private val outgoingFinishedRequestsCounter = Counter.builder("outgoing.finished.requests")
        .tag("account", accountName).register(meterRegistry)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = Int.MAX_VALUE
            maxRequestsPerHost = Int.MAX_VALUE
        })
        .connectionPool(ConnectionPool(properties.parallelRequests, 20, TimeUnit.SECONDS))
        .protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(properties.rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = NonBlockingOngoingWindow(properties.parallelRequests)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {

        val cf = CompletableFuture<Boolean>()
        val transactionId = UUID.randomUUID()
        incomingRequestsCounter.increment()

        // non-blocking parallel limit
        val winResp = ongoingWindow.putIntoWindow()
        if (winResp is NonBlockingOngoingWindow.WindowResponse.Fail) {
            logSubmission(false, paymentId, transactionId, paymentStartedAt)
            incomingFinishedRequestsCounter.increment()
            cf.complete(false)
            return cf
        }

        // non-blocking rate limit
        if (!rateLimiter.tick()) {
            logSubmission(false, paymentId, transactionId, paymentStartedAt)
            ongoingWindow.releaseWindow()
            incomingFinishedRequestsCounter.increment()
            cf.complete(false)
            return cf
        }

        // successful submission
        logSubmission(true, paymentId, transactionId, paymentStartedAt)

        if (System.currentTimeMillis() >= deadline) {
            logProcessing(false, paymentId, transactionId, "Deadline exceeded before send")
            ongoingWindow.releaseWindow()
            incomingFinishedRequestsCounter.increment()
            cf.complete(false)
            return cf
        }

        val url = "http://$paymentProviderHostPort/external/process" +
                "?timeout=PT60S&serviceName=${properties.serviceName}&token=$token" +
                "&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"

        val request = Request.Builder()
            .url(url)
            .post(emptyBody)
            .build()

        outgoingRequestsCounter.increment()

        val callClient = client.newBuilder()
            .callTimeout((deadline - System.currentTimeMillis()).coerceAtLeast(1000), TimeUnit.MILLISECONDS)
            .build()

        callClient.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                logProcessing(false, paymentId, transactionId, "IO error: ${e.message}")
                ongoingWindow.releaseWindow()
                outgoingFinishedRequestsCounter.increment()
                incomingFinishedRequestsCounter.increment()
                cf.complete(false)
            }

            override fun onResponse(call: Call, response: Response) {
                response.use {
                    val bodyStr = response.body?.string() ?: ""
                    val success = try {
                        val resp = mapper.readValue(bodyStr, ExternalSysResponse::class.java)
                        logProcessing(resp.result, paymentId, transactionId, resp.message)
                        resp.result
                    } catch (ex: Exception) {
                        logProcessing(false, paymentId, transactionId, "Parse error, code=${response.code}")
                        false
                    }

                    ongoingWindow.releaseWindow()
                    outgoingFinishedRequestsCounter.increment()
                    incomingFinishedRequestsCounter.increment()
                    cf.complete(success)
                }
            }
        })

        return cf
    }

    private fun logSubmission(success: Boolean, paymentId: UUID, transactionId: UUID, startedAt: Long) {
        val spent = Duration.ofMillis(System.currentTimeMillis() - startedAt)
        paymentESService.update(paymentId) {
            it.logSubmission(success, transactionId, System.currentTimeMillis(), spent)
        }
    }

    private fun logProcessing(success: Boolean, paymentId: UUID, transactionId: UUID, reason: String?) {
        paymentESService.update(paymentId) {
            it.logProcessing(success, System.currentTimeMillis(), transactionId, reason)
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

private fun now() = System.currentTimeMillis()