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

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        400,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            maximumPoolSize = 400
            removeOnCancelPolicy = true
            rejectedExecutionHandler = CallerBlockingRejectedExecutionHandler(Duration.ofMinutes(30))
        }
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        paymentExecutor.execute {
            val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }

        return createdAt
    }

    @jakarta.annotation.PreDestroy
    fun shutdown() {
        paymentExecutor.shutdownNow()
    }
}