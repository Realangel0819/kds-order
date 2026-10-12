package pengyu.order.event;

import pengyu.order.Order;

import java.time.LocalDateTime;

/**
 * 주문이 COMPLETED로 바뀌었을 때 Kafka(order-completed-topic)로 나가는 이벤트.
 * Consumer가 다른 서비스를 다시 조회하지 않고 바로 색인할 수 있도록 검색에 필요한 값을 모두 싣는다.
 */
public record OrderCompletedEvent(
        Long orderId,
        Long menuId,
        String menuName,
        Integer totalPrice,
        String status,
        LocalDateTime createdAt
) {
    public static OrderCompletedEvent from(Order order) {
        // 주문 시점 단가가 없는 예전 주문은 금액을 비워 둔다 (0원으로 집계되면 통계가 틀어짐)
        Integer totalPrice = order.getPrice() == null ? null : order.getPrice() * order.getQuantity();
        return new OrderCompletedEvent(
                order.getId(),
                order.getMenuId(),
                order.getMenuName(),
                totalPrice,
                order.getStatus().name(),
                order.getCreatedAt());
    }
}
