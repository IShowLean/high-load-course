package ru.quipy.common.utils

import java.time.Duration

enum class CompositeMode {
    AND,
    OR
}

class CompositeRateLimiter(
    private val rl1: RateLimiter,
    private val rl2: RateLimiter,
    private val mode: CompositeMode = CompositeMode.AND
) : RateLimiter {
    override fun tick(): Boolean {
        return when (mode) {
            CompositeMode.AND -> rl1.tick() && rl2.tick()
            CompositeMode.OR -> rl1.tick() || rl2.tick()
        }
    }

    override fun tickBlocking() {
        when (mode) {
            CompositeMode.AND -> {
                rl1.tickBlocking()
                rl2.tickBlocking()
            }
            CompositeMode.OR -> {
                if (rl1.tick()) return
                if (rl2.tick()) return
                if (rl1.tickBlocking(Duration.ofMillis(50))) return
                rl2.tickBlocking()
            }
        }
    }

    override fun tickBlocking(timeout: Duration): Boolean {
        if (timeout.toMillis() <= 0) return false

        return when (mode) {
            CompositeMode.AND -> {
                // Нужно чтобы оба прошли в рамках timeout; не делим фиксированно, а оцениваем остаток
                val deadline = System.currentTimeMillis() + timeout.toMillis()
                val firstOk = rl1.tickBlocking(remaining(deadline))
                if (!firstOk) return false
                rl2.tickBlocking(remaining(deadline))
            }

            CompositeMode.OR -> {
                // быстрая попытка
                if (rl1.tick()) return true
                if (rl2.tick()) return true

                val deadline = System.currentTimeMillis() + timeout.toMillis()

                val short = Duration.ofMillis( minOf(50L, timeout.toMillis()) )
                if (rl1.tickBlocking(short)) return true
                val timeLeftAfterFirst = remaining(deadline)
                if (timeLeftAfterFirst.toMillis() <= 0) return false
                if (rl2.tickBlocking(timeLeftAfterFirst)) return true

                false
            }
        }
    }

    private fun remaining(deadlineMillis: Long): Duration {
        val left = deadlineMillis - System.currentTimeMillis()
        return if (left > 0) Duration.ofMillis(left) else Duration.ZERO
    }
}