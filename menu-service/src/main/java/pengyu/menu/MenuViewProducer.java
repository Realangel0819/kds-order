package pengyu.menu;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class MenuViewProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String TOPIC = "menu-view-topic";

    public void sendMenuViewEvent(Long menuId) {
        MenuViewEvent event = new MenuViewEvent(menuId, LocalDateTime.now());

        // 파티션 Key로 menuId를 사용해 같은 메뉴의 이벤트 순서를 보장
        kafkaTemplate.send(TOPIC, String.valueOf(menuId), event);

        log.info("메뉴 조회 이벤트 발행 완료: menuId={}", menuId);
    }
}