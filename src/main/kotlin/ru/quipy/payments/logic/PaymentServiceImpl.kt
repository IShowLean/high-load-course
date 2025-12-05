package ru.quipy.payments.logic

import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.collections.toTypedArray

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>
) : PaymentService {

    override fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {

        val enabledAccounts = paymentAccounts.filter { it.isEnabled() }
        if (enabledAccounts.isEmpty()) {
            return CompletableFuture.completedFuture(false)
        }

        val futures = enabledAccounts.map { account ->
            account.performPaymentAsync(paymentId, amount, paymentStartedAt, deadline)
        }

        return CompletableFuture.allOf(*futures.toTypedArray<CompletableFuture<*>>())
            .thenApply {
                futures.any { it.join() }
            }
            .exceptionally { false }
    }
}