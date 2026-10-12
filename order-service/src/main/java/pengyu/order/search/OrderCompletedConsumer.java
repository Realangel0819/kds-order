package pengyu.order.search;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pengyu.order.event.OrderCompletedEvent;
import pengyu.order.event.OrderCompletedEventPublisher;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderCompletedConsumer {

    private final OrderSearchRepository orderSearchRepository;
    private final ObjectMapper objectMapper;

    // groupId는 테스트에서 다른 그룹으로 바꿀 수 있도록 프로퍼티로 뺀다 (기본값 order-search-group)
    @KafkaListener(topics = OrderCompletedEventPublisher.TOPIC, groupId = "${search.consumer.group-id:order-search-group}")
    public void consumeOrderCompletedEvent(String message) throws JsonProcessingException {
        // 조회수(MenuViewConsumer)와 달리 주문 이력은 유실되면 검색/통계가 틀어지므로, 실패를 여기서 삼키지 않는다.
        // 예외를 던지면 KafkaErrorHandlerConfig의 에러 핸들러가 받는다.
        // - 역직렬화 실패(JsonProcessingException): 다시 읽어도 똑같이 실패하므로 재시도 없이 바로 DLT로
        // - ES 저장 실패: 간격을 늘려가며 재시도하고, 끝까지 실패하면 DLT로
        OrderCompletedEvent event = objectMapper.readValue(message, OrderCompletedEvent.class);

        // 문서 ID = 주문 ID라서 재시도/중복 수신/재색인 배치로 여러 번 저장돼도 같은 문서를 덮어쓸 뿐이다 (멱등).
        orderSearchRepository.save(OrderDocument.from(event));
        log.info("[Kafka Consumer] ES 색인 완료 - orderId={}, menuName={}", event.orderId(), event.menuName());
    }
}
