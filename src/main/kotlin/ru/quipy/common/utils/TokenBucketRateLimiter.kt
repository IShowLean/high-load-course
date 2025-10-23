package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.max

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
) : RateLimiter {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(TokenBucketRateLimiter::class.java)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private val bucket: AtomicInteger = AtomicInteger(0)
    @Volatile private var start = System.currentTimeMillis()
    @Volatile private var nextExpectedWakeUp = start + timeUnit.toMillis(window)

    // монитор для ожидания появления токенов
    private val monitor = Object()

    // job пополнения токенов
    @Suppress("JoinDeclarationAndAssignment")
    private val releaseJob = rateLimiterScope.launch {
        while (true) {
            start = System.currentTimeMillis()
            nextExpectedWakeUp = start + timeUnit.toMillis(window)

            val cur = bucket.get()
            val delta = if (cur + rate > bucketMaxCapacity) bucketMaxCapacity - cur else rate
            if (delta > 0) {
                bucket.addAndGet(delta)
                // разбудим ожидающих
                synchronized(monitor) {
                    monitor.notifyAll()
                }
            }

            val sleep = nextExpectedWakeUp - System.currentTimeMillis()
            if (sleep > 0) {
                delay(sleep)
            } else {
                // если ушли в минус из-за планирования/часов — просто перейдём к следующей итерации
                // (delay(0) тоже валиден, но тут и не обязателен)
            }
        }
    }.also { job ->
        job.invokeOnCompletion { th ->
            if (th != null) logger.error("Rate limiter release job completed exceptionally", th)
        }
    }

    override fun tick(): Boolean {
        while (true) {
            val tokensAvailable = bucket.get()
            if (tokensAvailable <= 0) return false
            if (bucket.compareAndSet(tokensAvailable, tokensAvailable - 1)) return true
        }
    }

    /**
     * Блокирует поток, пока не появится токен.
     * Уважает прерывание: восстанавливает флаг interrupt, но продолжает ждать до получения токена.
     */
    override fun tickBlocking() {
        while (true) {
            // быстрая попытка забрать токен без блокировки
            while (true) {
                val tokens = bucket.get()
                if (tokens > 0 && bucket.compareAndSet(tokens, tokens - 1)) {
                    return
                }
                if (tokens <= 0) break // нет токенов — переходим к ожиданию
            }

            // ждём до ближайшего пополнения или нотификации
            val waitMillis = max(1L, nextExpectedWakeUp - System.currentTimeMillis())
            synchronized(monitor) {
                if (bucket.get() <= 0) {
                    try {
                        monitor.wait(waitMillis)
                    } catch (ie: InterruptedException) {
                        // восстанавливаем флаг и продолжаем ожидание,
                        // чтобы семантика "обязательно получить токен" сохранилась
                        Thread.currentThread().interrupt()
                    }
                }
            }
        }
    }

    /**
     * Пытается получить токен, блокируясь максимум на [timeout].
     * Возвращает true, если токен получен, иначе false по таймауту.
     * Если поток прервали во время ожидания — восстанавливает флаг и возвращает false.
     */
    override fun tickBlocking(timeout: Duration): Boolean {
        val deadlineNanos = System.nanoTime() + timeout.toNanos()

        while (true) {
            // быстрая попытка без блокировки
            while (true) {
                val tokens = bucket.get()
                if (tokens > 0 && bucket.compareAndSet(tokens, tokens - 1)) {
                    return true
                }
                if (tokens <= 0) break
            }

            val remainingNanos = deadlineNanos - System.nanoTime()
            if (remainingNanos <= 0) return false

            val untilRefillMillis = max(1L, nextExpectedWakeUp - System.currentTimeMillis())
            val remainingMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos)
            val waitMillis = max(1L, minOf(untilRefillMillis, remainingMillis))

            synchronized(monitor) {
                if (bucket.get() <= 0) {
                    try {
                        monitor.wait(waitMillis)
                    } catch (ie: InterruptedException) {
                        Thread.currentThread().interrupt()
                        return false
                    }
                }
            }
        }
    }
}