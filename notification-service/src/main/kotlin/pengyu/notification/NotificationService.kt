package pengyu.notification

import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.util.concurrent.Queues

/**
 * 주문 알림 발행 서비스.
 * Kafka Consumer(주문 완료 이벤트)와 테스트용 POST API가 알림을 넣고, KDS SSE 구독자가 꺼내 간다.
 *
 * 내부 파이프는 Sink다. Sink는 외부에서 tryEmitNext()로 데이터를 밀어 넣으면,
 * asFlux()로 구독 중인 쪽에 흘려보내는 "입구 달린 Flux"다.
 */
@Service
class NotificationService {

    // multicast(): 구독자(KDS) 여럿이 같은 알림을 동시에 받는 브로드캐스트 방식
    // autoCancel=false: 기본값(true)이면 마지막 KDS가 연결을 끊는 순간 Sink가 종료돼서 이후 알림을 영영 못 보낸다.
    private val sink: Sinks.Many<NotificationMessage> =
        Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false)

    // 주문 완료 이벤트를 KDS 화면에 띄울 알림으로 바꿔서 발행한다
    fun notifyOrderCompleted(event: OrderCompletedEvent): Sinks.EmitResult =
        publish(
            NotificationMessage(
                menuId = event.menuId,
                message = "주문 #${event.orderId} ${event.menuName ?: "메뉴 ${event.menuId}"} 조리 완료",
            )
        )

    // tryEmitNext는 동시에 두 스레드가 호출하면 FAIL_NON_SERIALIZED로 실패한다.
    // Kafka 리스너 스레드와 POST 요청 스레드가 동시에 넣을 수 있으므로 한 번에 하나씩만 넣는다.
    // tryEmitNext는 예외 대신 결과 코드를 돌려주므로 실패 처리는 호출하는 쪽이 정한다.
    @Synchronized
    fun publish(message: NotificationMessage): Sinks.EmitResult = sink.tryEmitNext(message)

    // KDS 구독용 스트림
    fun stream(): Flux<NotificationMessage> = sink.asFlux()
}
