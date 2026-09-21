package pengyu.menu;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.format.DateTimeFormatter;

@Slf4j
@Component
@RequiredArgsConstructor
public class MenuViewConsumer {

    private final MenuViewRepository menuViewRepository;
    private final ObjectMapper objectMapper;

    // TODO(개선): 지금은 단건 처리 - 메시지 1개당 DynamoDB save() 1번 (피크 시 초당 수만 건이면 네트워크 호출 폭증).
    //   Kafka @KafkaListener를 BatchListener로 바꿔서 N개씩 묶어 받고,
    //   DynamoDB batchWriteItem으로 한 번에 밀어 넣는 구조로 개선할 것.
    @KafkaListener(topics = "menu-view-topic", groupId = "menu-group")
    public void consumeMenuViewEvent(String message) {
        try {
            MenuViewEvent event = objectMapper.readValue(message, MenuViewEvent.class);

            MenuViewEntity entity = new MenuViewEntity(
                    event.menuId(),
                    event.viewedAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    "test-user");

            try {
                menuViewRepository.save(entity);
                log.info("[Kafka Consumer] DynamoDB 적재 완료 = menuId={}, viewedAt={}", entity.getMenuId(), entity.getViewedAt());
            } catch (Exception e) {
                // DynamoDB 저장 실패가 리스너 예외로 전파되면 같은 메시지가 무한 재시도되며 컨슈머가 멈출 수 있으므로,
                // 실패를 여기서 삼키고 다음 메시지로 넘어간다. (조회수 집계는 일부 유실을 감수할 수 있는 데이터)
                log.error("[Kafka Consumer] DynamoDB 적재 실패 = menuId={}, viewedAt={}", entity.getMenuId(), entity.getViewedAt(), e);
            }
        } catch (JsonProcessingException e) {
            log.error("[Kafka Consumer] 메시지 역직렬화 실패 = {}", message, e);
        }
    }
}
