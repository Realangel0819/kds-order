package pengyu.menu;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MenuViewConsumer {

    @KafkaListener(topics = "menu-view-topic", groupId = "menu-group")
    public void consumeMenuViewEvent(String message) {

        // 2. 메시지가 도착하면 로그를 찍어서 확인합니다.
        // (Day 3에서는 이 부분을 지우고 DynamoDB에 저장하는 로직이 들어갑니다!)
        log.info("[Kafka Consumer] 메뉴 조회 이벤트 수신 완료 = {}", message);
    }
}