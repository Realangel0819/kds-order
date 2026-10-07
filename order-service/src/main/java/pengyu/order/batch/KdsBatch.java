package pengyu.order.batch;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * KDS로 한 번에 내려보내는 조리 묶음.
 *
 * @param firstOrderedAt 묶음 안에서 가장 먼저 들어온 주문 시각 (= 가장 오래 기다린 주문)
 * @param groupId        묶음 ID. 이 묶음에 포함된 주문들의 Order.groupId로 저장된다
 * @param orderIds       이 묶음에 포함된 주문 ID 목록
 */
public record KdsBatch(Long menuId, int quantity, Instant firstOrderedAt, Instant sentAt, FlushTrigger trigger,
                       String groupId, List<Long> orderIds) {

    // 묶음 안에서 가장 오래 기다린 주문의 대기 시간
    public Duration maxWait() {
        return Duration.between(firstOrderedAt, sentAt);
    }
}
