package pengyu.order.search;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pengyu.order.Order;
import pengyu.order.OrderRepository;
import pengyu.order.OrderService;
import pengyu.order.OrderStatus;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 주문 완료 -> Kafka -> Consumer -> ES 색인 -> 키워드 검색 전체 흐름 확인.
 * 로컬 docker-compose의 MySQL, Kafka, Elasticsearch가 떠 있어야 한다.
 */
@SpringBootTest
class OrderSearchIntegrationTest {

    private static final long INDEX_TIMEOUT_MS = 15_000;

    @Autowired
    private OrderService orderService;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private OrderSearchRepository orderSearchRepository;

    @Autowired
    private OrderSearchService orderSearchService;

    private final List<Long> createdOrderIds = new ArrayList<>();

    @AfterEach
    void tearDown() {
        createdOrderIds.forEach(id -> {
            orderRepository.deleteById(id);
            orderSearchRepository.deleteById(String.valueOf(id));
        });
    }

    @Test
    @DisplayName("COMPLETED가 된 주문만 ES에 색인되고, '치킨'으로 검색하면 '양념 치킨'/'후라이드 치킨' 주문이 나온다")
    void completedOrder_isIndexed_andSearchableByKeyword() throws InterruptedException {
        Long yangnyeom = saveOrder("양념 치킨", 18000, 2);
        Long fried = saveOrder("후라이드 치킨", 16000, 1);
        Long stew = saveOrder("김치찌개", 9000, 1);
        Long cooking = saveOrder("간장 치킨", 19000, 1);

        orderService.updateOrderStatus(yangnyeom, OrderStatus.COMPLETED);
        orderService.updateOrderStatus(fried, OrderStatus.COMPLETED);
        orderService.updateOrderStatus(stew, OrderStatus.COMPLETED);
        orderService.updateOrderStatus(cooking, OrderStatus.COOKING); // 완료 전이라 색인되면 안 됨

        waitUntilIndexed(yangnyeom, fried, stew);

        OrderSearchResponse response = orderSearchService.searchByMenuName("치킨", "COMPLETED", 0, 100);
        List<Long> foundIds = response.orders().stream().map(OrderSearchResponse.Item::orderId).toList();

        assertThat(foundIds).contains(yangnyeom, fried).doesNotContain(stew, cooking);
        assertThat(orderSearchRepository.existsById(String.valueOf(cooking))).isFalse();

        OrderSearchResponse.Item item = response.orders().stream()
                .filter(o -> o.orderId().equals(yangnyeom))
                .findFirst().orElseThrow();
        assertThat(item.totalPrice()).isEqualTo(36000); // 단가 18000 x 수량 2
        assertThat(item.status()).isEqualTo("COMPLETED");
        assertThat(item.createdAt()).isNotNull();
    }

    private Long saveOrder(String menuName, int price, int quantity) {
        Order order = orderRepository.save(Order.builder()
                .menuId(1L)
                .menuName(menuName)
                .price(price)
                .quantity(quantity)
                .status(OrderStatus.PENDING)
                .build());
        createdOrderIds.add(order.getId());
        return order.getId();
    }

    // Kafka -> Consumer -> ES는 비동기라서, 문서가 생길 때까지 잠깐씩 기다리며 확인한다
    private void waitUntilIndexed(Long... orderIds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + INDEX_TIMEOUT_MS;
        for (Long orderId : orderIds) {
            while (!orderSearchRepository.existsById(String.valueOf(orderId))) {
                assertThat(System.currentTimeMillis()).as("orderId=%d 색인 대기 시간 초과", orderId).isLessThan(deadline);
                Thread.sleep(200);
            }
        }
    }
}
