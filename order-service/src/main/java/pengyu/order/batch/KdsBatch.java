package pengyu.order.batch;

import java.time.Duration;
import java.time.Instant;

/**
 * KDS로 한 번에 내려보내는 조리 묶음.
 *
 * @param firstOrderedAt 묶음 안에서 가장 먼저 들어온 주문 시각 (= 가장 오래 기다린 주문)
 */
public record KdsBatch(Long menuId, int quantity, Instant firstOrderedAt, Instant sentAt, FlushTrigger trigger) {

    // 묶음 안에서 가장 오래 기다린 주문의 대기 시간
    public Duration maxWait() {
        return Duration.between(firstOrderedAt, sentAt);
    }
}
