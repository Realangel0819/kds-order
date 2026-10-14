package pengyu.notification

import java.time.LocalDateTime

/**
 * order-service가 order-completed-topic으로 발행하는 주문 완료 이벤트 (order-service의 OrderCompletedEvent record와 같은 모양).
 * 서비스 간에 클래스를 공유하지 않고 JSON 계약만 맞춘다. 모르는 필드는 Boot의 ObjectMapper가 무시한다.
 */
data class OrderCompletedEvent(
    val orderId: Long,
    val menuId: Long,
    val menuName: String?,
    val totalPrice: Int?, // 주문 시점 단가가 없는 예전 주문은 null
    val status: String,
    val createdAt: LocalDateTime?,
)
