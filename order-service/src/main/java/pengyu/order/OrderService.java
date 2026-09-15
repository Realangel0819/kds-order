package pengyu.order;

import com.kds.menu.grpc.MenuGrpcServiceGrpc;
import com.kds.menu.grpc.MenuRequest;
import com.kds.menu.grpc.MenuResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final MenuGrpcServiceGrpc.MenuGrpcServiceBlockingStub menuServiceBlockingStub;

    @Value("${grpc.client.menu-service.timeout-seconds}")
    private long menuServiceTimeoutSeconds;

    // 1. 주문 생성
    @Transactional
    public Long createOrder(Long menuId, int quantity) {
        // Menu Service가 지연/다운되더라도 여기서 방어막(타임아웃)을 통과하거나 예외로 즉시 탈출한다.
        MenuResponse menu = fetchMenuInfo(menuId);
        log.info("메뉴 조회 완료 - menuId={}, menuName={}", menuId, menu.getMenuName());

        Order order = Order.builder()
                .menuId(menuId)
                .quantity(quantity)
                .status(OrderStatus.PENDING) // 처음 주문 시 무조건 대기 상태
                .build();

        Order savedOrder = orderRepository.save(order);
        return savedOrder.getId(); // 생성된 주문 ID 반환
    }

    private MenuResponse fetchMenuInfo(Long menuId) {
        MenuRequest request = MenuRequest.newBuilder()
                .setMenuId(menuId)
                .build();

        String threadName = Thread.currentThread().getName();
        long start = System.currentTimeMillis();
        try {
            // 호출 시점마다 새로 데드라인을 건다 (withDeadlineAfter는 호출할 때마다 새 stub을 반환함)
            MenuResponse response = menuServiceBlockingStub
                    .withDeadlineAfter(menuServiceTimeoutSeconds, TimeUnit.SECONDS)
                    .getMenuInfo(request);

            log.info("[gRPC] Menu Service 응답 수신 - menuId={}, elapsed={}ms, thread={}",
                    menuId, System.currentTimeMillis() - start, threadName);
            return response;
        } catch (StatusRuntimeException e) {
            long elapsed = System.currentTimeMillis() - start;
            if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED) {
                // [방어 성공] 스레드가 묶이지 않고 데드라인(설정한 타임아웃) 시점에 즉시 탈출한다.
                log.warn("[gRPC] Menu Service 응답 지연으로 타임아웃 - menuId={}, elapsed={}ms, thread={}",
                        menuId, elapsed, threadName);
                throw new IllegalStateException(
                        "메뉴 서비스 응답 지연으로 주문을 처리할 수 없습니다. 잠시 후 다시 시도해주세요.", e);
            }
            log.error("[gRPC] Menu Service 통신 오류 - menuId={}, elapsed={}ms, thread={}, status={}",
                    menuId, elapsed, threadName, e.getStatus());
            throw new RuntimeException("메뉴 서비스 통신 중 오류가 발생했습니다: " + e.getStatus(), e);
        }
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