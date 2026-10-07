package pengyu.order.stat;

import jakarta.persistence.EntityManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import pengyu.order.Order;
import pengyu.order.OrderRepository;
import pengyu.order.OrderStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reader만 떼어서 검증한다. (Job/Step 없이 open -> read -> close 를 직접 호출)
 * - @DataJpaTest는 내장 H2로 바꿔 띄우므로 개발용 order_db를 건드리지 않는다.
 */
@DataJpaTest
// @DataJpaTest는 기본으로 테스트를 트랜잭션으로 감싸고 끝나면 롤백한다.
// 그런데 Reader는 자기 EntityManager(= 다른 커넥션)로 읽기 때문에, 커밋 안 된 테스트 데이터가 안 보인다.
// 그래서 트랜잭션을 끄고 save()마다 바로 커밋되게 한 뒤, @AfterEach에서 직접 지운다.
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class DailyCookStatReaderTest {

    private static final LocalDate TARGET_DATE = LocalDate.of(2026, 10, 5);
    private static final LocalDateTime YESTERDAY_NOON = TARGET_DATE.atTime(12, 0);

    @Autowired
    EntityManagerFactory emf;

    @Autowired
    OrderRepository orderRepository;

    JpaPagingItemReader<CookStatRow> reader;

    @BeforeEach
    void setUp() throws Exception {
        // 메뉴 1: 3건짜리 묶음 2개 -> 주문 6건, 조리 2번
        saveCompleted(1L, "g1", YESTERDAY_NOON, 3);
        saveCompleted(1L, "g2", YESTERDAY_NOON, 3);
        // 메뉴 2: 2건짜리 묶음 1개 -> 주문 2건, 조리 1번 (그중 1건은 자정 정각 = 시작 경계, 포함되어야 함)
        saveCompleted(2L, "g3", TARGET_DATE.atStartOfDay(), 1);
        saveCompleted(2L, "g3", YESTERDAY_NOON, 1);

        // --- 아래는 전부 집계에서 빠져야 하는 주문 ---
        orderRepository.save(order(1L, OrderStatus.CANCELED, "g1", YESTERDAY_NOON)); // 완료되지 않음
        saveCompleted(1L, null, YESTERDAY_NOON, 1);                                    // 묶이지 않음
        saveCompleted(1L, "g9", TARGET_DATE.plusDays(1).atStartOfDay(), 1);            // 오늘 자정 정각 = 끝 경계, 제외
        saveCompleted(3L, "g8", TARGET_DATE.atStartOfDay().minusSeconds(1), 1);        // 그저께 23:59:59

        reader = DailyCookStatBatchConfig.createReader(emf, TARGET_DATE);
        reader.afterPropertiesSet(); // 빌더로 만든 Reader는 직접 호출해 줘야 필수값 검증/초기화가 된다
    }

    @AfterEach
    void tearDown() {
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("기준일자의 COMPLETED + 묶인 주문만 메뉴별로 집계해서 읽어온다")
    void readsAggregatedRowsPerMenu() throws Exception {
        reader.open(new ExecutionContext());

        List<CookStatRow> rows = new ArrayList<>();
        CookStatRow row;
        while ((row = reader.read()) != null) { // null = 더 읽을 게 없음 (Step이 종료를 판단하는 기준)
            rows.add(row);
        }
        reader.close();

        assertThat(rows).containsExactly(
                new CookStatRow(1L, 6, 2),
                new CookStatRow(2L, 2, 1));
    }

    private void saveCompleted(Long menuId, String groupId, LocalDateTime createdAt, int count) {
        for (int i = 0; i < count; i++) {
            orderRepository.save(order(menuId, OrderStatus.COMPLETED, groupId, createdAt));
        }
    }

    private Order order(Long menuId, OrderStatus status, String groupId, LocalDateTime createdAt) {
        return Order.builder()
                .menuId(menuId)
                .quantity(1)
                .status(status)
                .groupId(groupId)
                .createdAt(createdAt)
                .build();
    }
}
