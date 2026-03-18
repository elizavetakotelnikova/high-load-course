package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CallNotPermittedException
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.pow


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val esExecutor = Executors.newFixedThreadPool(32)
    private val scheduler = Executors.newScheduledThreadPool(100)
    private val semaphore = java.util.concurrent.Semaphore(properties.parallelRequests)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val paymentRequestsCounter = Counter.builder("payment.requests.incoming")
        .description("Total payment requests received by adapter")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val paymentSuccessCounter = Counter.builder("payment.requests.processed")
        .description("Total payment requests successfully processed")
        .tag("outcome", "success")
        .register(Metrics.globalRegistry)

    private val paymentErrorCounter = Counter.builder("payment.requests.processed")
        .description("Total payment requests failed")
        .tag("outcome", "error")
        .register(Metrics.globalRegistry)
    private val requestLatency = Timer.builder("payment.request.latency.seconds")
        .description("Payment request latency in seconds")
        .tags("adapter", "payment")
        .publishPercentiles(0.5, 0.85, 0.9, 0.95, 0.99)
        .register(Metrics.globalRegistry)
    private val paymentRetryCounter = Counter.builder("payment.requests.retries")
        .description("Total number of retried payment requests")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val http2Client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(100))
        .version(HttpClient.Version.HTTP_2)
        .build()
    val timeoutTime = properties.averageProcessingTime.toMillis() * 2
    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val maxAttempts = 3
    private val maxDelayMs = 100L
    private val delayBaseMs = 5L

    private val circuitBreaker = CircuitBreaker.of(
        accountName,
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(20)
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .waitDurationInOpenState(Duration.ofSeconds(4))
            .permittedNumberOfCallsInHalfOpenState(6)
            .build()
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }
        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
        performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, 1)
    }

    private fun performRequestWithRetry(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        if (attempt > maxAttempts) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Deadline exceeded or max attempts reached")
            }
            return
        }

        if (!slidingWindowRateLimiter.tickBlocking(Duration.ofMillis(deadline - now()))) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Rate limit exceed")
            }
            return
        }

        val timeToBlock = deadline - System.currentTimeMillis()
        val acquired = semaphore.tryAcquire(timeToBlock, TimeUnit.MILLISECONDS)
        if (!acquired) {
            logger.warn("[$accountName] Timeout acquiring semaphore for payment $paymentId")
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Semaphore timeout")
            }
            return
        }

        val request = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .timeout(Duration.ofMillis(1800))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val startTime = now()

        circuitBreaker.decorateCompletionStage {
            http2Client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
        }.get()
            .thenApply { response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                val duration = now() - startTime
                paymentRequestsCounter.increment()
                requestLatency.record(duration, TimeUnit.MILLISECONDS)

                if (body.result) {
                    paymentSuccessCounter.increment()
                    paymentESService.update(paymentId) {
                        it.logProcessing(true, now(), transactionId, reason = body.message)
                    }
                    true
                } else {
                    paymentErrorCounter.increment()
                    false
                }
            }
            .handle { result, ex ->
                semaphore.release()
                val duration = now() - startTime
                requestLatency.record(duration, TimeUnit.MILLISECONDS)
                if (ex != null) {

                    when (ex) {
                        is CallNotPermittedException -> {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Circuit breaker open")
                            }
                            return@handle null
                        }

                        is SocketTimeoutException -> {
                            return@handle retryOrFinalFail(
                                paymentId, amount, transactionId,
                                paymentStartedAt, deadline, attempt,
                                "Request timeout"
                            )
                        }

                        else -> {
                            return@handle retryOrFinalFail(
                                paymentId, amount, transactionId,
                                paymentStartedAt, deadline, attempt,
                                ex.message ?: "Unknown error"
                            )
                        }
                    }
                }
                if (result == false) {
                    return@handle retryOrFinalFail(
                        paymentId, amount, transactionId,
                        paymentStartedAt, deadline, attempt,
                        "Business failure"
                    )
                }
                null
            }
    }

    private fun retryOrFinalFail(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int,
        reason: String
    ): Void? {

        if (attempt < maxAttempts) {
            val delay = exponentialBackoffDelay(attempt)
            val remaining = deadline - now()
            if (remaining > delay + 50) {
                paymentRetryCounter.increment()
                scheduleRetry(delay) {
                    performRequestWithRetry(
                        paymentId,
                        amount,
                        transactionId,
                        paymentStartedAt,
                        deadline,
                        attempt + 1
                    )
                }
                return null
            }
        }
        paymentErrorCounter.increment()
        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = reason)
        }

        return null
    }

    private fun exponentialBackoffDelay(attempt: Int): Long {
        return minOf((delayBaseMs * 2.0.pow((attempt - 1).toDouble())).toLong(), maxDelayMs)
    }

    private fun scheduleRetry(
        delayMs: Long,
        task: () -> Unit
    ) {
        scheduler.schedule(task, delayMs, TimeUnit.MILLISECONDS)
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()