package pengyu.order.stat;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;

// 아래 벌크 UPDATE/DELETE는 이를 호출하는 Tasklet Step의 트랜잭션을 그대로 쓴다.
// (3개를 한 트랜잭션에서 실행해야 "기존 ACTIVE -> 새 ACTIVE" 교체가 원자적으로 일어난다)
public interface DailyCookStatRepository extends JpaRepository<DailyCookStat, Long> {

    List<DailyCookStat> findByStatDateAndStatusOrderByMenuId(LocalDate statDate, DailyCookStatStatus status);

    // 1) 현재 ACTIVE인 통계를 INACTIVE로 내린다 (지우지 않고 이력으로 남김)
    @Modifying
    @Query("""
            UPDATE DailyCookStat s SET s.status = pengyu.order.stat.DailyCookStatStatus.INACTIVE
            WHERE s.statDate = :statDate AND s.status = pengyu.order.stat.DailyCookStatStatus.ACTIVE
            """)
    int deactivate(@Param("statDate") LocalDate statDate);

    // 2) 이전에 실패한 실행이 남긴 PENDING 찌꺼기를 지운다 (어차피 ACTIVE가 될 일이 없는 행)
    @Modifying
    @Query("""
            DELETE FROM DailyCookStat s
            WHERE s.statDate = :statDate AND s.status = pengyu.order.stat.DailyCookStatStatus.PENDING
              AND s.jobExecutionId <> :jobExecutionId
            """)
    int deleteStalePending(@Param("statDate") LocalDate statDate, @Param("jobExecutionId") Long jobExecutionId);

    // 3) 이번 실행이 넣은 PENDING을 ACTIVE로 올린다
    @Modifying
    @Query("""
            UPDATE DailyCookStat s SET s.status = pengyu.order.stat.DailyCookStatStatus.ACTIVE
            WHERE s.statDate = :statDate AND s.status = pengyu.order.stat.DailyCookStatStatus.PENDING
              AND s.jobExecutionId = :jobExecutionId
            """)
    int activate(@Param("statDate") LocalDate statDate, @Param("jobExecutionId") Long jobExecutionId);
}
