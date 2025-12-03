package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import kotlin.math.max

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry
) {
    companion object {
        private val logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        500,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            maximumPoolSize = 500
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            removeOnCancelPolicy = true
        }
    }

    private val bucketQueue = LeakingBucketRateLimiter(rate = 2000, window = Duration.ofSeconds(1), bucketSize = 8000)

    private val paymentRetryCounter = Counter.builder("payment.retries").register(meterRegistry)
    private val retryOpportunityCounter = Counter.builder("payment.retry.opportunity").register(meterRegistry)
    private val requestLatency = Timer.builder("request_latency")
        .publishPercentiles(0.5, 0.8, 0.9, 0.99)
        .register(meterRegistry)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        if (!bucketQueue.tick()) {
            return null
        }

        paymentExecutor.submit {
            val event = paymentESService.create { it.create(paymentId, orderId, amount) }
            logger.trace("Payment ${event.paymentId} created for order $orderId")
            retryAsync(paymentId, amount, createdAt, deadline, 1)
        }
        return createdAt
    }

    private fun retryAsync(paymentId: UUID, amount: Int, createdAt: Long, deadline: Long, attempt: Int) {
        if (System.currentTimeMillis() >= deadline) return

        val start = System.currentTimeMillis()
        val future = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)

        future
            .orTimeout(max(1, deadline - System.currentTimeMillis()), TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                requestLatency.record(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS)

                val failed = error != null || success != true
                if (failed) {
                    paymentRetryCounter.increment()
                    if (deadline - System.currentTimeMillis() > 2000) {
                        retryOpportunityCounter.increment()
                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt + 1)
                    }
                }
            }, paymentExecutor)
    }

    private fun scheduleRetry(paymentId: UUID, amount: Int, createdAt: Long, deadline: Long, attempt: Int) {
        val timeLeft = deadline - System.currentTimeMillis()
        if (timeLeft <= 0) return

        val baseBackoff = (100L shl (attempt - 1)).coerceAtMost(2000L)
        val jitter = ThreadLocalRandom.current().nextLong(0, 100L)
        val delayMs = (baseBackoff + jitter).coerceAtMost(timeLeft - 500)

        paymentExecutor.schedule({
            retryAsync(paymentId, amount, createdAt, deadline, attempt)
        }, delayMs, TimeUnit.MILLISECONDS)
    }
}