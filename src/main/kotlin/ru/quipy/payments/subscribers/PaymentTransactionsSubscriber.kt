package ru.quipy.payments.subscribers

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import jakarta.annotation.PostConstruct
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

@Service
class PaymentTransactionsSubscriber(
    private val meterRegistry: MeterRegistry,
) {
    private val logger = LoggerFactory.getLogger(PaymentTransactionsSubscriber::class.java)

    val paymentLog: MutableMap<UUID, MutableList<PaymentLogRecord>> = ConcurrentHashMap()

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    private val timeToFirstSuccessTimer: Timer = Timer.builder("payment_time_to_first_success_seconds")
        .description("Time from submission to first SUCCESS processing for a paymentId")
        .publishPercentileHistogram()
        .publishPercentiles(0.9, 0.95, 0.99)
        .register(meterRegistry)

    private val duplicateSuccessCounter: Counter = Counter.builder("payment_duplicate_success_total")
        .description("Number of duplicate SUCCESS events for the same paymentId")
        .register(meterRegistry)

    private fun processedCounter(result: String, reason: String): Counter =
        Counter.builder("payment_processed_total")
            .description("Number of processed payment events")
            .tag("result", result)
            .tag("reason", reason)
            .register(meterRegistry)

    private val attemptsToSuccessSummary: DistributionSummary = DistributionSummary.builder("payment_attempts_to_success")
        .description("How many PaymentProcessedEvent attempts happened before the first SUCCESS for a paymentId")
        .baseUnit("attempts")
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val attemptsByPaymentId = ConcurrentHashMap<UUID, AtomicInteger>()

    private val successSeenByPaymentId = ConcurrentHashMap<UUID, Boolean>()

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "payments:payment-processings-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(PaymentProcessedEvent::class) { event ->
                paymentLog.computeIfAbsent(event.orderId) { CopyOnWriteArrayList() }
                    .add(
                        PaymentLogRecord(
                            event.processedAt,
                            status = if (event.success) PaymentStatus.SUCCESS else PaymentStatus.FAILED,
                            event.amount,
                            event.paymentId,
                        )
                    )

                val attempts = attemptsByPaymentId
                    .computeIfAbsent(event.paymentId) { AtomicInteger(0) }
                    .incrementAndGet()

                val reason = (event.reason ?: "none").ifBlank { "none" }
                    .take(80)
                if (event.success) {
                    processedCounter("success", "none").increment()
                } else {
                    processedCounter("fail", reason).increment()
                }

                if (event.success) {
                    val first = successSeenByPaymentId.putIfAbsent(event.paymentId, true) == null
                    if (first) {
                        val durMs = (event.processedAt - event.submittedAt).coerceAtLeast(0L)
                        timeToFirstSuccessTimer.record(Duration.ofMillis(durMs))
                        attemptsToSuccessSummary.record(attempts.toDouble())

                        attemptsByPaymentId.remove(event.paymentId)
                    } else {
                        duplicateSuccessCounter.increment()
                        logger.warn("Duplicate SUCCESS for paymentId=${event.paymentId} orderId=${event.orderId}")
                    }
                }
            }
        }
    }

    class PaymentLogRecord(
        val timestamp: Long,
        val status: PaymentStatus,
        val amount: Int,
        val transactionId: UUID,
    )

    enum class PaymentStatus {
        FAILED,
        SUCCESS
    }
}