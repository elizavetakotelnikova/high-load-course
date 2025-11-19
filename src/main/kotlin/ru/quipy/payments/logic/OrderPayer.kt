package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.RateLimitExceededException
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(50, Duration.ofSeconds(1))

    private val scope = CoroutineScope(Dispatchers.IO)

    suspend fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()

        if (!slidingWindowRateLimiter.tick()) {
            throw RateLimitExceededException()
        }

        val createdEvent = paymentESService.create {
            it.create(paymentId, orderId, amount)
        }
        logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

        scope.launch {
            try {
                paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
            } catch (ex: Exception) {
                logger.error("Payment submission failed for $paymentId", ex)
            }
        }

        return createdAt
    }
}