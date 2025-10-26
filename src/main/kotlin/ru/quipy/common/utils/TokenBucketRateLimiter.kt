package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import java.time.Duration
import kotlin.math.min

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val ticksPerSecond: Int = 50
) : RateLimiter {

    companion object {
        private val logger: Logger = LoggerFactory.getLogger(TokenBucketRateLimiter::class.java)
    }

    private val scope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val bucket = AtomicInteger(bucketMaxCapacity)

    init {
        val refillPerTick = rate.toDouble() / ticksPerSecond
        scope.launch {
            var carry = 0.0
            while (true) {
                carry += refillPerTick
                val add = carry.toInt()
                if (add > 0) {
                    bucket.get().let { cur ->
                        val toAdd = min(bucketMaxCapacity - cur, add)
                        if (toAdd > 0) bucket.addAndGet(toAdd)
                    }
                    carry -= add
                }
                delay(1000L / ticksPerSecond)
            }
        }
    }

    override fun tick(): Boolean {
        while (true) {
            val cur = bucket.get()
            if (cur <= 0) return false
            if (bucket.compareAndSet(cur, cur - 1)) return true
        }
    }

    override fun tickBlocking() {
        while (!tick()) {
            Thread.sleep(5)
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        val end = System.currentTimeMillis() + timeout.toMillis()
        while (System.currentTimeMillis() <= end) {
            if (tick()) return true
            Thread.sleep(5)
        }
        return false
    }
}