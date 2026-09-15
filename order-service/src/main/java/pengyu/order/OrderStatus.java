package pengyu.order;

public enum OrderStatus {
    PENDING,   // 주문 접수 대기
    COOKING,   // 조리 중
    COMPLETED, // 조리 완료
    CANCELED   // 주문 취소
}
