package pengyu.order.search;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import pengyu.order.Order;
import pengyu.order.OrderRepository;
import pengyu.order.OrderStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * "이벤트 발행이 유실돼 ES에 없는 주문"을 재색인 배치가 DB 기준으로 다시 채우는지 검증한다.
 * - DB/ES 모두 로컬 docker-compose. H2를 쓰면 ID가 1부터 다시 시작해서 ES에 이미 있는 문서와 ID가 겹친다.
 *   MySQL auto-increment는 ID를 재사용하지 않으므로 겹치지 않고, 정리도 이 테스트가 만든 ID만 지운다.
 * - 주문을 repository로 직접 저장하면 이벤트가 나가지 않으므로 "발행 유실" 상황이 그대로 재현된다
 */
@SpringBootTest
class OrderReindexJobTest {

    // 다른 테스트/개발 데이터와 겹치지 않는 과거 날짜
    private static final LocalDate TARGET_DATE = LocalDate.of(2020, 1, 15);

    @Autowired
    OrderReindexJobRunner jobRunner;

    @Autowired
    OrderRepository orderRepository;

    @Autowired
    OrderSearchRepository orderSearchRepository;

    private final List<Long> createdOrderIds = new ArrayList<>();

    @AfterEach
    void tearDown() {
        createdOrderIds.forEach(id -> {
            orderRepository.deleteById(id);
            orderSearchRepository.deleteById(String.valueOf(id));
        });
    }

    @Test
    @DisplayName("그날 완료된 주문 중 ES에 없는 주문을 다시 색인하고, 다른 날짜/미완료 주문은 건드리지 않는다")
    void reindexesCompletedOrdersOfTargetDate() throws Exception {
        // 전날 밤에 주문했지만 대상일에 완료 -> 포함 (createdAt이 아니라 completedAt 기준)
        Long lateNight = save(OrderStatus.COMPLETED, "양념 치킨", TARGET_DATE.minusDays(1).atTime(23, 50), TARGET_DATE.atTime(0, 20));
        Long noon = save(OrderStatus.COMPLETED, "후라이드 치킨", TARGET_DATE.atTime(12, 0), TARGET_DATE.atTime(12, 30));
        // 대상일 다음날 완료 -> 제외
        Long nextDay = save(OrderStatus.COMPLETED, "간장 치킨", TARGET_DATE.atTime(23, 50), TARGET_DATE.plusDays(1).atTime(0, 10));
        // 아직 조리 중 -> 제외
        Long cooking = save(OrderStatus.COOKING, "김치찌개", TARGET_DATE.atTime(12, 0), null);

        assertThat(orderSearchRepository.existsById(String.valueOf(noon))).isFalse(); // 발행 유실 상태

        JobExecution execution = jobRunner.run(TARGET_DATE);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(orderSearchRepository.existsById(String.valueOf(lateNight))).isTrue();
        assertThat(orderSearchRepository.existsById(String.valueOf(noon))).isTrue();
        assertThat(orderSearchRepository.existsById(String.valueOf(nextDay))).isFalse();
        assertThat(orderSearchRepository.existsById(String.valueOf(cooking))).isFalse();

        OrderDocument document = orderSearchRepository.findById(String.valueOf(noon)).orElseThrow();
        assertThat(document.getMenuName()).isEqualTo("후라이드 치킨");
        assertThat(document.getTotalPrice()).isEqualTo(20000); // 단가 10000 x 수량 2
        assertThat(document.getStatus()).isEqualTo("COMPLETED");
    }

    @Test
    @DisplayName("같은 날짜를 두 번 돌려도 문서가 늘지 않는다 (문서 ID = 주문 ID라 덮어쓰기)")
    void rerunIsIdempotent() throws Exception {
        Long noon = save(OrderStatus.COMPLETED, "양념 치킨", TARGET_DATE.atTime(12, 0), TARGET_DATE.atTime(12, 30));
        jobRunner.run(TARGET_DATE);
        long afterFirst = orderSearchRepository.count();

        jobRunner.run(TARGET_DATE);

        assertThat(orderSearchRepository.existsById(String.valueOf(noon))).isTrue();
        assertThat(orderSearchRepository.count()).isEqualTo(afterFirst);
    }

    private Long save(OrderStatus status, String menuName, LocalDateTime createdAt, LocalDateTime completedAt) {
        Order order = orderRepository.save(Order.builder()
                .menuId(1L)
                .menuName(menuName)
                .price(10000)
                .quantity(2)
                .status(status)
                .createdAt(createdAt)
                .completedAt(completedAt)
                .build());
        createdOrderIds.add(order.getId());
        return order.getId();
    }
}
