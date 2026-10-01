package pengyu.order.batch;

/**
 * 배치가 KDS로 나간 이유.
 */
public enum FlushTrigger {
    SIZE, // 누적 수량이 임계치에 도달해서 즉시 전송 (물량 방어)
    TIME  // 윈도우 시간이 지나서 자투리를 강제 전송 (시간 방어)
}
