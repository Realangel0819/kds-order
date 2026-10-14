package pengyu.notification

import io.kotest.core.spec.style.BehaviorSpec
import io.kotest.matchers.shouldBe
import reactor.test.StepVerifier
import java.time.Duration
import java.time.LocalDateTime

class NotificationServiceTest : BehaviorSpec({

    Given("KDS 2대가 알림을 구독 중일 때") {
        val service = NotificationService()

        When("알림을 발행하면") {
            val message = NotificationMessage(menuId = 1L, message = "주문 #1 짜장면", timestamp = LocalDateTime.now())

            Then("두 KDS 모두 같은 알림을 받아야 한다") {
                StepVerifier.create(service.stream().mergeWith(service.stream()))
                    .then { service.publish(message).isSuccess shouldBe true }
                    .expectNext(message, message) // data class라 equals로 필드 값을 비교한다
                    .thenCancel()
                    .verify(Duration.ofSeconds(5))
            }
        }

        When("메뉴 이름이 없는 예전 주문의 완료 이벤트가 오면") {
            val event = OrderCompletedEvent(
                orderId = 7L, menuId = 3L, menuName = null,
                totalPrice = null, status = "COMPLETED", createdAt = null,
            )

            Then("메뉴 ID로 대신 표시한 알림이 발행되어야 한다") {
                StepVerifier.create(service.stream())
                    .then { service.notifyOrderCompleted(event) }
                    .assertNext { message ->
                        message.menuId shouldBe 3L
                        message.message shouldBe "주문 #7 메뉴 3 조리 완료"
                    }
                    .thenCancel()
                    .verify(Duration.ofSeconds(5))
            }
        }
    }
})
