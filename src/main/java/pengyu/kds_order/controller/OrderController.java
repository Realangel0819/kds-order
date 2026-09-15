package pengyu.kds_order.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import pengyu.kds_order.domain.Order;
import pengyu.kds_order.domain.OrderStatus;
import pengyu.kds_order.service.OrderService;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    // 주문 생성 API (POST)
    @PostMapping
    public Long createOrder(@RequestParam Long menuId, @RequestParam int quantity) {
        return orderService.createOrder(menuId, quantity);
    }

    // 주문 상태 변경 API (PATCH) - 예: 대기(PENDING) -> 조리중(COOKING)
    @PatchMapping("/{orderId}/status")
    public String updateOrderStatus(@PathVariable Long orderId, @RequestParam OrderStatus status) {
        orderService.updateOrderStatus(orderId, status);
        return "주문 ID " + orderId + "의 상태가 " + status + "(으)로 변경되었습니다.";
    }

    // 주문 단건 조회 API (GET)
    @GetMapping("/{orderId}")
    public Order getOrder(@PathVariable Long orderId) {
        return orderService.getOrder(orderId);
    }

    // 주문 전체 조회 API (GET)
    @GetMapping
    public List<Order> getOrders() {
        return orderService.getOrders();
    }

    // 주문 삭제 API (DELETE)
    @DeleteMapping("/{orderId}")
    public String deleteOrder(@PathVariable Long orderId) {
        orderService.deleteOrder(orderId);
        return "주문 ID " + orderId + "가 삭제되었습니다.";
    }
}