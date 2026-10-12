package pengyu.order.event;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCompletedEventPublisher {

    public static final String TOPIC = "order-completed-topic";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    // 상태 변경 트랜잭션이 커밋된 뒤에만 발행한다 (롤백된 "가짜 완료"가 검색 인덱스에 들어가는 것 방지)
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void publish(OrderCompletedEvent event) {
        // 파티션 Key로 orderId를 사용해 같은 주문의 이벤트 순서를 보장
        kafkaTemplate.send(TOPIC, String.valueOf(event.orderId()), event)
                .whenComplete((result, e) -> {
                    if (e != null) {
                        // 발행 실패해도 주문 상태(DB)는 이미 COMPLETED. 검색 인덱스만 누락되므로 로그로 남겨 재색인 대상으로 삼는다
                        log.error("[Kafka Producer] 주문 완료 이벤트 발행 실패 - orderId={}", event.orderId(), e);
                    } else {
                        log.info("[Kafka Producer] 주문 완료 이벤트 발행 완료 - orderId={}", event.orderId());
                    }
                });
    }
}
