package ru.quipy.payments.logic

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.ScheduledThreadPoolExecutor

@Service
class OrderPayer(
    private val meterRegistry: MeterRegistry,
) {
    companion object {
        private val logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }


    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val inFlight = java.util.concurrent.Semaphore(200)

    private val paymentExecutor = java.util.concurrent.ThreadPoolExecutor(
        64,
        64,
        0L,
        java.util.concurrent.TimeUnit.MILLISECONDS,
        java.util.concurrent.LinkedBlockingQueue<Runnable>(5_000),
        NamedThreadFactory("payment-http-executor"),
        java.util.concurrent.ThreadPoolExecutor.AbortPolicy()
    ).apply { prestartAllCoreThreads() }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long? {
        val createdAt = System.currentTimeMillis()

        // если перегруз - сразу отказываем, чтобы API вернул 429
        if (!inFlight.tryAcquire(200, java.util.concurrent.TimeUnit.MILLISECONDS)) {
            return null
        }

        return try {
            paymentExecutor.execute {
                try {
                    val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                    paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
                } catch (t: Throwable) {
                    logger.warn("Payment task failed paymentId=$paymentId orderId=$orderId: ${t.message}", t)
                } finally {
                    inFlight.release()
                }
            }
            createdAt
        } catch (_: java.util.concurrent.RejectedExecutionException) {
            inFlight.release()
            null
        }
    }

    @jakarta.annotation.PreDestroy
    fun shutdown() {
        paymentExecutor.shutdownNow()
    }
}