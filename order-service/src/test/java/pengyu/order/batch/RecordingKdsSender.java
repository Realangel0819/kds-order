package pengyu.order.batch;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * KDS로 나간 배치를 기록해두는 가짜 Sender (여러 스레드에서 동시에 호출되므로 thread-safe 컬렉션 사용)
 */
class RecordingKdsSender implements KdsSender {

    private final ConcurrentLinkedQueue<KdsBatch> sent = new ConcurrentLinkedQueue<>();

    @Override
    public void send(KdsBatch batch) {
        sent.add(batch);
    }

    List<KdsBatch> sent() {
        return List.copyOf(sent);
    }

    int totalQuantity() {
        return sent.stream().mapToInt(KdsBatch::quantity).sum();
    }
}
