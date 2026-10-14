package pengyu.notification

import java.time.LocalDateTime

/**
 * KDS로 내려보내는 알림 한 건.
 * data class라서 equals/hashCode/toString/copy를 컴파일러가 만들어 준다 (Java의 @Getter, @AllArgsConstructor 등이 필요 없음).
 * val만 써서 한 번 만들어진 알림은 바뀌지 않는다 (여러 구독자에게 같은 객체가 동시에 전달돼도 안전).
 */
data class NotificationMessage(
    val menuId: Long,
    val message: String,
    // 기본값: 생략하면 알림을 만든 시각. (jackson-module-kotlin이 기본값을 인식해서 JSON에 없어도 역직렬화된다)
    val timestamp: LocalDateTime = LocalDateTime.now(),
)
