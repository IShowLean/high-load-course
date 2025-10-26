package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.*

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    // Тест 2: 11 rps, 100 - 3 мин. 30+ сек.; Тест 3: 100 rps, 300 - 5 мин.
    private val intakeRateLimitPerSec = 100
    private val queueCapacity = 300
    private val paymentQueue = LinkedBlockingQueue<Runnable>(queueCapacity)

    private val paymentExecutor = ThreadPoolExecutor(
        16, 16,
        0L, TimeUnit.MILLISECONDS,
        paymentQueue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val inboundLimiter: RateLimiter = CompositeRateLimiter(
        rl1 = SlidingWindowRateLimiter(rate = intakeRateLimitPerSec.toLong(), window = Duration.ofSeconds(1)),
        rl2 = TokenBucketRateLimiter(
            rate = intakeRateLimitPerSec,
            bucketMaxCapacity = intakeRateLimitPerSec * 2,
            ticksPerSecond = 20
        ),
        mode = CompositeMode.OR
    )

    private val acceptedRequestsCounter: Counter = Counter
        .builder("incoming.payments.accepted")
        .register(meterRegistry)

    private fun reject(reason: String, retryAfterMillis: Long): Nothing {
        logger.debug("Rejecting payment due to $reason, retryAfter=${retryAfterMillis}ms")
        Counter.builder("incoming.payments.rejected")
            .tag("reason", reason)
            .register(meterRegistry)
            .increment()
        throw TooManyRequestsException(System.currentTimeMillis() + retryAfterMillis)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val now = System.currentTimeMillis()
        val budget = deadline - now
        if (budget <= 0) reject("deadline", 500)

        val remainingCap = paymentExecutor.queue.remainingCapacity()
        if (remainingCap <= 2) reject("deadline", 500)

        val waitTime = if (paymentExecutor.queue.size > queueCapacity * 0.7) 30 else 80
        val allowed = inboundLimiter.tickBlocking(Duration.ofMillis(waitTime.toLong()))

        if (!allowed) reject("limiter_blocked", 250)


        acceptedRequestsCounter.increment()

        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(paymentId, orderId, amount)
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, now, deadline)
                } catch (ex: Exception) {
                    logger.error("Error in payment submission task", ex)
                }
            }
        } catch (e: TooManyRequestsException) {
            throw e
        } catch (e: RejectedExecutionException) {
            reject("executor_rejected", 400)
        }

        return now
    }

    class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many incoming requests")
}