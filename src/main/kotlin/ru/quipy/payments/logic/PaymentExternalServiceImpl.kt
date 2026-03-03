package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
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
import kotlin.math.min
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
    private val maxDelayMs = 1000L
    private val delayBaseMs = 50L

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
        if (now() + requestAverageProcessingTime.toMillis() > deadline || attempt > maxAttempts) {
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
            //.timeout(Duration.ofMillis(150))
            .POST(HttpRequest.BodyPublishers.noBody())
            .build()

        val startTime = now()
        http2Client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { response ->
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

            if (body.result) {
                paymentSuccessCounter.increment()
                semaphore.release()
            } else {
                paymentErrorCounter.increment()
                if (attempt > 1) {
                    paymentRetryCounter.increment()
                }
                semaphore.release()
                val currentDelay = exponentialBackoffDelay(attempt)
                val remainingTime = deadline - now()
                val sleepTime = min(currentDelay, remainingTime - 50)
                if (sleepTime > 0) {
                    scheduleRetry(sleepTime) {
                        performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                    }
                }
            }

        }.exceptionally { ex ->
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
            if (attempt > 1) {
                paymentRetryCounter.increment()
            }
            requestLatency.record(now() - startTime, TimeUnit.MILLISECONDS)
            val currentDelay = exponentialBackoffDelay(attempt)
            val remainingTime = deadline - now()
            val sleepTime = min(currentDelay, remainingTime - 50)
            if (sleepTime > 0) {
                scheduleRetry(sleepTime) {
                    performRequestWithRetry(paymentId, amount, transactionId, paymentStartedAt, deadline, attempt + 1)
                }
            }
        }
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