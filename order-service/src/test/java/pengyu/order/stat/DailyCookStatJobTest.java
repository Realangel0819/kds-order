package pengyu.order.stat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import pengyu.order.Order;
import pengyu.order.OrderRepository;
import pengyu.order.OrderStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static pengyu.order.stat.DailyCookStatStatus.*;

/**
 * Job 전체(집계 PENDING 적재 -> ACTIVE 교체)를 실제로 돌려서 통계 테이블에 적재되는지 검증한다.
 * - @AutoConfigureTestDatabase: DataSource를 내장 H2로 바꿔치기 (개발용 order_db를 건드리지 않음)
 * - RedissonClient: 빈 생성 시 Redis에 바로 접속하므로 Mock으로 대체 (이 테스트는 분산락을 쓰지 않음)
 * - DailyCookStatRepository: 교체 Step 실패를 재현하려고 SpyBean으로 감싼다 (기본은 실제 메서드 호출)
 */
@SpringBootTest
@AutoConfigureTestDatabase
class DailyCookStatJobTest {

    private static final LocalDate TARGET_DATE = LocalDate.of(2026, 10, 5);
    private static final LocalDateTime TARGET_NOON = TARGET_DATE.atTime(12, 0);

    @MockBean
    RedissonClient redissonClient;

    @SpyBean
    DailyCookStatRepository dailyCookStatRepository;

    @Autowired
    DailyCookStatJobRunner jobRunner;

    @Autowired
    OrderRepository orderRepository;

    @AfterEach
    void tearDown() {
        dailyCookStatRepository.deleteAll();
        orderRepository.deleteAll();
    }

    @Test
    @DisplayName("배치를 실행하면 메뉴별 주문 건수/실제 조리 횟수/절감 횟수가 ACTIVE로 적재된다")
    void loadsDailyStats() throws Exception {
        // 메뉴 1: 3건짜리 묶음 2개 -> 주문 6, 조리 2, 절감 4
        saveCompleted(1L, "g1", TARGET_NOON, 3);
        saveCompleted(1L, "g2", TARGET_NOON, 3);
        // 메뉴 2: 1건짜리 묶음 1개 -> 묶음 효과 없음 (절감 0)
        saveCompleted(2L, "g3", TARGET_NOON, 1);
        // 다른 날짜 주문은 제외
        saveCompleted(1L, "g4", TARGET_NOON.plusDays(1), 5);

        JobExecution execution = jobRunner.run(TARGET_DATE);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(active())
                .extracting(DailyCookStat::getMenuId, DailyCookStat::getTotalOrderCount,
                        DailyCookStat::getActualCookCount, DailyCookStat::getSavedCookCount,
                        DailyCookStat::getJobExecutionId)
                .containsExactly(
                        tuple(1L, 6, 2, 4, execution.getId()),
                        tuple(2L, 1, 1, 0, execution.getId()));
        assertThat(dailyCookStatRepository.findAll()).allMatch(s -> s.getStatus() == ACTIVE); // PENDING 잔여 없음
    }

    @Test
    @DisplayName("같은 날짜를 다시 실행하면 새 결과만 ACTIVE가 되고, 기존 결과는 INACTIVE로 남는다")
    void rerunReplacesActive() throws Exception {
        saveCompleted(1L, "g1", TARGET_NOON, 3);
        JobExecution first = jobRunner.run(TARGET_DATE);

        // 첫 실행 이후 늦게 완료 처리된 주문이 생긴 상황
        saveCompleted(1L, "g2", TARGET_NOON, 2);
        JobExecution rerun = jobRunner.run(TARGET_DATE);

        assertThat(rerun.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(active())
                .extracting(DailyCookStat::getTotalOrderCount, DailyCookStat::getActualCookCount,
                        DailyCookStat::getSavedCookCount, DailyCookStat::getJobExecutionId)
                .containsExactly(tuple(5, 2, 3, rerun.getId()));
        assertThat(dailyCookStatRepository.findAll())
                .extracting(DailyCookStat::getJobExecutionId, DailyCookStat::getStatus)
                .containsExactlyInAnyOrder(
                        tuple(first.getId(), INACTIVE),
                        tuple(rerun.getId(), ACTIVE));
    }

    @Test
    @DisplayName("재처리가 교체 도중 실패해도 기존 ACTIVE 통계는 그대로 남고, 다음 실행이 잔여 PENDING을 정리한다")
    void failureKeepsPreviousActive() throws Exception {
        saveCompleted(1L, "g1", TARGET_NOON, 3);
        JobExecution first = jobRunner.run(TARGET_DATE);
        saveCompleted(1L, "g2", TARGET_NOON, 2);

        // 교체 Step의 마지막 쿼리(activate)에서 실패 -> 같은 트랜잭션의 deactivate도 롤백되어야 한다
        doThrow(new IllegalStateException("DB 장애 가정"))
                .when(dailyCookStatRepository).activate(any(), anyLong());
        JobExecution failed = jobRunner.run(TARGET_DATE);

        assertThat(failed.getStatus()).isEqualTo(BatchStatus.FAILED);
        // 조회 API 기준: 첫 실행 결과가 그대로 보인다 (유실도, 반쯤 바뀐 값도 없음)
        assertThat(active())
                .extracting(DailyCookStat::getTotalOrderCount, DailyCookStat::getJobExecutionId)
                .containsExactly(tuple(3, first.getId()));
        // 실패한 실행의 결과는 PENDING으로만 남아 있다
        assertThat(dailyCookStatRepository.findByStatDateAndStatusOrderByMenuId(TARGET_DATE, PENDING))
                .extracting(DailyCookStat::getJobExecutionId)
                .containsExactly(failed.getId());

        // 장애 복구 후 재실행
        reset(dailyCookStatRepository);
        JobExecution recovered = jobRunner.run(TARGET_DATE);

        assertThat(recovered.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        assertThat(active())
                .extracting(DailyCookStat::getTotalOrderCount, DailyCookStat::getJobExecutionId)
                .containsExactly(tuple(5, recovered.getId()));
        assertThat(dailyCookStatRepository.findAll())
                .extracting(DailyCookStat::getJobExecutionId, DailyCookStat::getStatus)
                .containsExactlyInAnyOrder(
                        tuple(first.getId(), INACTIVE),
                        tuple(recovered.getId(), ACTIVE)); // 실패한 실행의 PENDING은 삭제됨
    }

    private List<DailyCookStat> active() {
        return dailyCookStatRepository.findByStatDateAndStatusOrderByMenuId(TARGET_DATE, ACTIVE);
    }

    private void saveCompleted(Long menuId, String groupId, LocalDateTime createdAt, int count) {
        for (int i = 0; i < count; i++) {
            orderRepository.save(Order.builder()
                    .menuId(menuId)
                    .quantity(1)
                    .status(OrderStatus.COMPLETED)
                    .groupId(groupId)
                    .createdAt(createdAt)
                    .build());
        }
    }
}
