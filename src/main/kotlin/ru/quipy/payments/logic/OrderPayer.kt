package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.ceil
import kotlin.math.max

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

    private val queueCapacity = 400
    private val paymentQueue = LinkedBlockingQueue<Runnable>(queueCapacity)

    private val paymentExecutor = ThreadPoolExecutor(
        16, 16,
        0L, TimeUnit.MILLISECONDS,
        paymentQueue,
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    // ограничение rps внешней системы (взято из графика)
    private val serviceRps = 10.5
    // определяем шаг для вызовов
    private val slotIntervalMs = ceil(1000.0 / serviceRps).toLong()
    // время обработки одного запроса внешней системой
    private val serviceProcMs = 1000L
    // компенсатор
    private val safetyMs = 400L

    // Следующий доступный слот
    private val nextSlotMillis = AtomicLong(System.currentTimeMillis())

    private val pacerScheduler: ScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(NamedThreadFactory("payment-pacer"))

    private val admitLock = java.util.concurrent.locks.ReentrantLock()

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

        val reservedSlot: Long
        admitLock.lock()
        try {
            val base = max(System.currentTimeMillis(), nextSlotMillis.get())
            reservedSlot = base
            // резервируем следующий слот для следующих запросов
            nextSlotMillis.set(base + slotIntervalMs)

            // полный прогноз до завершения
            val totalWaitMs = (reservedSlot - now) + serviceProcMs + safetyMs

            if (totalWaitMs > budget) {
                val retryAfter = (totalWaitMs - budget + 500).coerceAtLeast(500)
                reject("queue_wait_exceeds_deadline", retryAfter)
            }

            acceptedRequestsCounter.increment()
        } finally {
            admitLock.unlock()
        }

        // Планируем фактическую отправку в зарезервированный момент
        val delay = max(0L, reservedSlot - System.currentTimeMillis())
        pacerScheduler.schedule({
            paymentExecutor.submit {
                val createdEvent = paymentESService.create { it.create(paymentId, orderId, amount) }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")
                paymentService.submitPaymentRequest(paymentId, amount, reservedSlot, deadline)
            }
        }, delay, TimeUnit.MILLISECONDS)

        return now
    }

    class TooManyRequestsException(val retryAfterMillis: Long) : RuntimeException("Too many incoming requests")
}
