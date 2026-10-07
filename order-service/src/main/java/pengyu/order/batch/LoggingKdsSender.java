package pengyu.order.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * KDS로 전송하는 가짜 구현체 (WebFlux/SSE로 실제 구현할 부분)
 */
@Slf4j
@Component
public class LoggingKdsSender implements KdsSender {

    @Override
    public void send(KdsBatch batch) {
        log.info("🔥 KDS 전송 완료: 메뉴 {} / 수량: {}개 한 번에 조리! (trigger={}, 최대 대기={}ms, groupId={}, orderIds={})",
                batch.menuId(), batch.quantity(), batch.trigger(), batch.maxWait().toMillis(), batch.groupId(), batch.orderIds());
    }
}
