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

    @KafkaListener(topics = "menu-view-topic", groupId = "menu-group")
    public void consumeMenuViewEvent(String message) {
        try {
            MenuViewEvent event = objectMapper.readValue(message, MenuViewEvent.class);

            MenuViewEntity entity = new MenuViewEntity(
                    event.menuId(),
                    event.viewedAt().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                    "test-user");

            menuViewRepository.save(entity);
            log.info("[Kafka Consumer] DynamoDB 적재 완료 = menuId={}, viewedAt={}", entity.getMenuId(), entity.getViewedAt());
        } catch (JsonProcessingException e) {
            log.error("[Kafka Consumer] 메시지 역직렬화 실패 = {}", message, e);
        }
    }
}
