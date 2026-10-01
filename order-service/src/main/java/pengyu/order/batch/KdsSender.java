package pengyu.order.batch;

/**
 * KDS 전송 추상화. 지금은 로그 출력 구현체만 있고, WebFlux/SSE 구현체로 교체할 예정.
 */
public interface KdsSender {

    void send(KdsBatch batch);
}
