package pengyu.order.search;

import java.time.LocalDateTime;
import java.util.List;

public record OrderSearchResponse(
        long totalHits, // 조건에 맞는 전체 건수 (현재 페이지 건수 아님)
        List<Item> orders
) {
    public record Item(
            Long orderId,
            Long menuId,
            String menuName,
            Integer totalPrice,
            String status,
            LocalDateTime createdAt
    ) {
        static Item from(OrderDocument document) {
            return new Item(
                    document.getOrderId(),
                    document.getMenuId(),
                    document.getMenuName(),
                    document.getTotalPrice(),
                    document.getStatus(),
                    document.getCreatedAt());
        }
    }
}
