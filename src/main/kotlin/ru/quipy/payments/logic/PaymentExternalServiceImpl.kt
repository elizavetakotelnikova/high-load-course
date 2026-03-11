package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import java.util.concurrent.CompletableFuture


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

    private val scheduler = Executors.newScheduledThreadPool(100)
    private val semaphore = java.util.concurrent.Semaphore(properties.parallelRequests)
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val retryAmount = 0
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

    private val hedgeTriggeredCounter = Counter.builder("payment.hedge.triggered")
        .description("Number of times hedge request was sent")
        .tag("adapter", "payment")
        .register(Metrics.globalRegistry)

    private val http2Client = HttpClient.newBuilder()
        .executor(Executors.newFixedThreadPool(128))
        .version(HttpClient.Version.HTTP_2)
        .build()

    val timeoutTime = properties.averageProcessingTime.toMillis() * 2
    val hedgeDelayMs = 700L
    val requestTimeoutMs = 2500L
    val slidingWindowRateLimiter = SlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofSeconds(1)
    )
    private val requestAverageProcessingTime = properties.averageProcessingTime

    private val circuitBreaker = CircuitBreaker.of(
        accountName,
        CircuitBreakerConfig.custom()
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
            .slidingWindowSize(20)
            .minimumNumberOfCalls(60)
            .failureRateThreshold(55f)
            .slowCallRateThreshold(65f)
            .slowCallDurationThreshold(Duration.ofMillis(1800))
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
        performPaymentWithHedge(paymentId, amount, transactionId, paymentStartedAt, deadline)
    }

    private fun performPaymentWithHedge(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        paymentStartedAt: Long,
        deadline: Long
    ) {
        if (!circuitBreaker.tryAcquirePermission()) {
            paymentErrorCounter.increment()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Circuit breaker open")
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

        val idempotencyKey = transactionId.toString()
        fun createRequest(): HttpRequest = HttpRequest.newBuilder()
            .uri(URI("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
            .header("x-idempotency-key", idempotencyKey)
            .timeout(Duration.ofMillis(requestTimeoutMs))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val startTime = now()
        val futures = mutableListOf<CompletableFuture<HttpResponse<String>>>()
        futures.add(http2Client.sendAsync(createRequest(), HttpResponse.BodyHandlers.ofString()))

        for (i in 1..retryAmount) {
            scheduler.schedule({
                if (futures.any { it.isDone }) return@schedule
                hedgeTriggeredCounter.increment()
                futures.add(http2Client.sendAsync(createRequest(), HttpResponse.BodyHandlers.ofString()))
            }, hedgeDelayMs * i, TimeUnit.MILLISECONDS)
        }

        CompletableFuture.anyOf(*futures.toTypedArray())
            .thenApply { winner ->
                winner as? HttpResponse<String> ?: throw RuntimeException("No response received")
            }
            .thenApply { response ->
                val body = try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentRequestsCounter.increment()
                requestLatency.record(now() - startTime, TimeUnit.MILLISECONDS)

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }

                val duration = now() - startTime
                if (body.result) {
                    circuitBreaker.onSuccess(duration, TimeUnit.MILLISECONDS)
                    paymentSuccessCounter.increment()
                    semaphore.release()
                } else {
                    circuitBreaker.onError(duration, TimeUnit.MILLISECONDS, RuntimeException("Payment failed"))
                    paymentErrorCounter.increment()
                    semaphore.release()
                }
        }.exceptionally { ex ->
                val duration = now() - startTime
                circuitBreaker.onError(duration, TimeUnit.MILLISECONDS, ex)
                when (ex) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", ex)
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = ex.message)
                        }
                    }
                }
                requestLatency.record(now() - startTime, TimeUnit.MILLISECONDS)
            }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()