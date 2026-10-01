package pengyu.order.batch;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 주문을 바로 KDS로 보내지 않고 메뉴별로 모아서 한 번에 조리하도록 묶는다.
 * - Size 방어: 누적 수량이 임계치에 도달하면 윈도우를 기다리지 않고 즉시 전송
 * - Time 방어: 윈도우마다 임계치에 못 미친 자투리 주문을 강제 전송
 */
@Slf4j
@Service
public class BatchGroupingService {

    // KDS로 보내기 위한 임시 바구니 (menuId -> 누적 수량 + 첫 주문 시각)
    private final Map<Long, PendingBatch> bucket = new ConcurrentHashMap<>();

    private final KdsSender kdsSender;
    private final Clock clock;
    private final int batchThreshold;

    public BatchGroupingService(KdsSender kdsSender,
                                Clock clock,
                                @Value("${kds.batch.threshold:5}") int batchThreshold) {
        this.kdsSender = kdsSender;
        this.clock = clock;
        this.batchThreshold = batchThreshold;
    }

    /**
     * 외부(주문 생성 이후)에서 주문이 들어왔을 때 호출하는 메서드
     */
    public void addOrder(Long menuId, int quantity) {
        Instant now = clock.instant();
        PendingBatch[] flushed = {null}; // 람다 밖으로 꺼내기 위한 홀더

        // 누적 -> 임계치 확인 -> 제거를 compute() 한 번으로 원자적으로 처리한다.
        // merge() 후 get()/remove()를 따로 부르면 그 사이에 들어온 주문이 유실되거나 중복 전송될 수 있다.
        bucket.compute(menuId, (id, current) -> {
            PendingBatch next = (current == null)
                    ? new PendingBatch(quantity, now)
                    : current.add(quantity);
            if (next.quantity() >= batchThreshold) {
                flushed[0] = next;
                return null; // null 반환 시 key가 제거된다
            }
            return next;
        });

        // 전송(I/O)은 compute()의 bin 락 밖에서 한다.
        if (flushed[0] != null) {
            send(menuId, flushed[0], FlushTrigger.SIZE);
        }
    }

    /**
     * 윈도우마다 자투리 주문을 강제로 비우는 메서드 (고정 윈도우)
     */
    @Scheduled(fixedRateString = "${kds.batch.window-ms:30000}")
    public void flushTimeWindowBatch() {
        // 복사 후 clear()는 그 사이에 들어온 주문을 날려버린다.
        // key마다 remove()로 "꺼내면서 지우기"를 원자적으로 해야 유실이 없다.
        for (Long menuId : bucket.keySet()) {
            PendingBatch pending = bucket.remove(menuId);
            if (pending != null) {
                send(menuId, pending, FlushTrigger.TIME);
            }
        }
    }

    private void send(Long menuId, PendingBatch pending, FlushTrigger trigger) {
        kdsSender.send(new KdsBatch(menuId, pending.quantity(), pending.firstOrderedAt(), clock.instant(), trigger));
    }

    private record PendingBatch(int quantity, Instant firstOrderedAt) {

        PendingBatch add(int more) {
            return new PendingBatch(quantity + more, firstOrderedAt);
        }
    }
}
