package pengyu.order;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;

    // 1. 주문 생성
    @Transactional
    public Long createOrder(Long menuId, int quantity) {
        Order order = Order.builder()
                .menuId(menuId)
                .quantity(quantity)
                .status(OrderStatus.PENDING) // 처음 주문 시 무조건 대기 상태
                .build();

        Order savedOrder = orderRepository.save(order);
        return savedOrder.getId(); // 생성된 주문 ID 반환
    }

    // 2. 주문 상태 변경 (Dirty Checking 적용)
    @Transactional
    public void updateOrderStatus(Long orderId, OrderStatus newStatus) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("해당 주문이 없습니다. id=" + orderId));

        // 객체 상태만 바꾸면 트랜잭션 커밋 시점에 UPDATE 쿼리가 나감!
        order.changeStatus(newStatus);
    }

    // 3. 주문 단건 조회
    @Transactional(readOnly = true)
    public Order getOrder(Long orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new IllegalArgumentException("해당 주문이 없습니다. id=" + orderId));
    }

    // 4. 주문 전체 조회
    @Transactional(readOnly = true)
    public List<Order> getOrders() {
        return orderRepository.findAll();
    }

    // 5. 주문 삭제
    @Transactional
    public void deleteOrder(Long orderId) {
        if (!orderRepository.existsById(orderId)) {
            throw new IllegalArgumentException("해당 주문이 없습니다. id=" + orderId);
        }
        orderRepository.deleteById(orderId);
    }
}