package pengyu.order;

import lombok.RequiredArgsConstructor;
import pengyu.order.batch.BatchGroupingService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final BatchGroupingService batchGroupingService;

    // 주문 생성 API (POST)
    @PostMapping
    public Long createOrder(@RequestParam Long menuId, @RequestParam int quantity) {
        Long orderId = orderService.createOrder(menuId, quantity);
        // 트랜잭션 커밋이 끝난 주문만 바구니에 담는다 (롤백된 주문이 주방으로 가는 것 방지)
        batchGroupingService.addOrder(menuId, quantity);
        return orderId;
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