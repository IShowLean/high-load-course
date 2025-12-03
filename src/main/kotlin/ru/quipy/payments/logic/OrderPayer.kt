package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val POOL_SIZE = 5000
    }

    @Autowired
    lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    lateinit var paymentService: PaymentService

    private val paymentRetryCounter = Counter.builder("payment.retries").register(meterRegistry)
    private val retryOpportunityCounter = Counter.builder("payment.retry.opportunity").register(meterRegistry)

    private val requestLatency = Timer.builder("request_latency")
        .description("Время выполнения попытки оплаты")
        .publishPercentiles(0.5, 0.8, 0.9, 0.99)
        .register(meterRegistry)

    private val paymentExecutor: ScheduledThreadPoolExecutor = object : ScheduledThreadPoolExecutor(
        POOL_SIZE,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            maximumPoolSize = POOL_SIZE
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            setRejectedExecutionHandler(java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy())
            removeOnCancelPolicy = true
        }
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            val event = paymentESService.create { it.create(paymentId, orderId, amount) }
            logger.trace("Payment ${event.paymentId} created for order $orderId")
            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (System.currentTimeMillis() >= deadline) return

        val attemptStart = System.currentTimeMillis()

        val future = paymentService.submitPaymentRequest(paymentId, amount, paymentStartedAt, deadline)

        future.whenCompleteAsync({ success, error ->
            val elapsed = System.currentTimeMillis() - attemptStart
            requestLatency.record(elapsed, TimeUnit.MILLISECONDS)

            val failed = error != null || success != true

            if (failed) {
                paymentRetryCounter.increment()
                val timeLeft = deadline - System.currentTimeMillis()
                if (timeLeft > 2000) {
                    retryOpportunityCounter.increment()
                    scheduleRetry(paymentId, amount, paymentStartedAt, deadline, attempt + 1)
                }
            }
        }, paymentExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val timeLeft = deadline - System.currentTimeMillis()
        if (timeLeft <= 0) return

        val baseBackoff = (100L shl (attempt - 1)).coerceAtMost(2000L)
        val jitter = ThreadLocalRandom.current().nextLong(0, 100)
        val delayMs = (baseBackoff + jitter).coerceAtMost(timeLeft - 500)

        paymentExecutor.schedule({
            retryAsync(paymentId, amount, paymentStartedAt, deadline, attempt)
        }, delayMs, TimeUnit.MILLISECONDS)
    }

    @jakarta.annotation.PreDestroy
    fun shutdown() {
        paymentExecutor.shutdownNow()
    }
}