package pengyu.notification

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class OrderCompletedConsumer(
    private val notificationService: NotificationService,
    private val objectMapper: ObjectMapper,
) {

    // order-service의 ES 색인 Consumer(order-search-group)와 다른 그룹이어야 두 쪽 모두 모든 메시지를 받는다.
    // 같은 그룹이면 파티션을 나눠 가져서 메시지가 한쪽에만 간다.
    @KafkaListener(topics = [TOPIC], groupId = "\${notification.consumer.group-id:order-notification-group}")
    fun consume(message: String) {
        // ES 색인과 달리 알림은 그 순간 화면에 띄우는 용도라, 실패해도 재시도/DLT 없이 로그만 남기고 넘어간다.
        // (재시도하는 동안 파티션이 막혀 뒤에 오는 알림까지 늦어지는 게 더 나쁘다)
        val event = try {
            objectMapper.readValue<OrderCompletedEvent>(message)
        } catch (e: JsonProcessingException) {
            log.error("[Kafka Consumer] 주문 완료 이벤트 역직렬화 실패 - 알림 생략: {}", message, e)
            return
        }

        val result = notificationService.notifyOrderCompleted(event)
        if (result.isFailure) {
            log.warn("[Kafka Consumer] 주문 알림 전송 실패 - orderId={}, result={}", event.orderId, result)
        } else {
            log.info("[Kafka Consumer] 주문 알림 전송 - orderId={}, menuName={}", event.orderId, event.menuName)
        }
    }

    companion object {
        const val TOPIC = "order-completed-topic"
        private val log = LoggerFactory.getLogger(OrderCompletedConsumer::class.java)
    }
}
