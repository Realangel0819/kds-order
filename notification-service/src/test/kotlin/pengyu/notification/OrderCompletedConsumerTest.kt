package pengyu.notification

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import reactor.test.StepVerifier
import java.time.Duration

// Kafka 없이 Consumer → NotificationService 연결만 검증한다 (리스너 메서드를 직접 호출)
class OrderCompletedConsumerTest : BehaviorSpec({

    // Boot의 ObjectMapper처럼 Kotlin 모듈 + JavaTimeModule(LocalDateTime)을 등록한다
    val objectMapper = jacksonObjectMapper().findAndRegisterModules()

    val validMessage = """
        {"orderId":1,"menuId":10,"menuName":"아메리카노","totalPrice":4500,"status":"COMPLETED","createdAt":"2026-10-14T09:30:00"}
    """.trimIndent()

    Given("KDS가 주문 알림을 구독 중일 때") {
        val notificationService = NotificationService()
        val consumer = OrderCompletedConsumer(notificationService, objectMapper)

        When("Kafka에서 주문 완료 이벤트를 받으면") {
            Then("NotificationMessage로 바뀌어 KDS에게 전달되어야 한다") {
                StepVerifier.create(notificationService.stream())
                    .then { consumer.consume(validMessage) }
                    .assertNext { message ->
                        message.menuId shouldBe 10L
                        message.message shouldBe "주문 #1 아메리카노 조리 완료"
                    }
                    .thenCancel()
                    .verify(Duration.ofSeconds(5))
            }
        }

        When("JSON 형식이 깨진 메시지를 받으면") {
            Then("예외 없이 건너뛰고, 다음 정상 메시지는 그대로 전달되어야 한다") {
                StepVerifier.create(notificationService.stream())
                    .then {
                        shouldNotThrowAny { consumer.consume("{깨진 JSON") }
                        consumer.consume(validMessage)
                    }
                    // 깨진 메시지는 Sink에 들어가지 않았으므로 첫 알림이 정상 메시지다
                    .assertNext { message -> message.message shouldBe "주문 #1 아메리카노 조리 완료" }
                    .thenCancel()
                    .verify(Duration.ofSeconds(5))
            }
        }
    }
})
