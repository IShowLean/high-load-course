package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var meterRegistry: MeterRegistry

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        250,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            setMaximumPoolSize(250)
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            setRejectedExecutionHandler(CallerBlockingRejectedExecutionHandler())
            setRemoveOnCancelPolicy(true)
        }
    }

    private val bucketQueue = LeakingBucketRateLimiter(rate = 2000, window = Duration.ofMillis(1000), bucketSize = 8000)

    private val requestLatency: Timer by lazy {
        Timer.builder("request_latency")
            .description("Request latency.")
            .publishPercentiles(0.5, 0.8, 0.9)
            .register(meterRegistry)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()
        if (!bucketQueue.tick()) {
            return null
        }

        paymentExecutor.submit {

            val createdEvent = paymentESService.create {
                it.create(paymentId, orderId, amount)
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now
        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val future = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)

        val start = System.currentTimeMillis()

        future
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->

                val elapsed = System.currentTimeMillis() - start
                requestLatency.record(elapsed, TimeUnit.MILLISECONDS)

                when {
                    error != null -> {

                        logger.warn(
                            "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                    "timeLeft=${deadline - System.currentTimeMillis()}ms"
                        )

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }

                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded")
                    }

                    success == false -> {

                        logger.info("Payment $paymentId attempt #$attempt returned failure")

                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, paymentExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now
        if (timeLeft <= 0) return

        val baseBackoff = (100L shl (attempt - 1)).coerceAtMost(2000L)
        val jitter = ThreadLocalRandom.current().nextLong(0, 100L)
        val delayMs = minOf(baseBackoff + jitter, timeLeft)

        paymentExecutor.schedule(
            {
                retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
            },
            delayMs,
            TimeUnit.MILLISECONDS
        )
    }


}