package pengyu.order.stat;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDate;

/**
 * 일일 조리 통계 배치(DailyCookStatJob)의 결과. (기준일자, 메뉴, 배치 실행)당 1행.
 * 재처리 중에도 기존 통계가 사라지지 않도록, 새 결과는 PENDING으로 넣고 마지막에 ACTIVE로 교체한다.
 */
@Entity
@Table(name = "daily_cook_stat",
        uniqueConstraints = {
                // 같은 실행 안에서 (날짜, 메뉴) 행이 중복으로 쌓이지 않도록 막는다
                // 실행이 다르면(재처리) 같은 (날짜, 메뉴)가 PENDING/ACTIVE/INACTIVE로 공존할 수 있다
                @UniqueConstraint(columnNames = {"stat_date", "menu_id", "job_execution_id"})
        },
        indexes = {
                // 조회 API(statDate + ACTIVE)와 교체 UPDATE가 타는 인덱스
                @Index(name = "idx_daily_cook_stat_date_status", columnList = "stat_date, status")
        })
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class DailyCookStat {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "stat_date", nullable = false)
    private LocalDate statDate; // 집계 기준일자 (보통 어제)

    @Column(name = "menu_id", nullable = false)
    private Long menuId;

    private int totalOrderCount; // 그날 들어온 주문 건수 (묶지 않았다면 이만큼 조리했어야 함)

    private int actualCookCount; // 실제 조리 사이클 횟수 (= 서로 다른 groupId 개수)

    private int savedCookCount; // 그룹핑으로 아낀 조리 횟수

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private DailyCookStatStatus status;

    @Column(name = "job_execution_id", nullable = false)
    private Long jobExecutionId; // 이 행을 적재한 배치 실행 ID (어떤 PENDING을 ACTIVE로 올릴지 구분하는 기준)

    @Builder
    public DailyCookStat(LocalDate statDate, Long menuId, int totalOrderCount, int actualCookCount, Long jobExecutionId) {
        this.statDate = statDate;
        this.menuId = menuId;
        this.totalOrderCount = totalOrderCount;
        this.actualCookCount = actualCookCount;
        this.savedCookCount = totalOrderCount - actualCookCount; // 생성 시점에 계산
        this.jobExecutionId = jobExecutionId;
        this.status = DailyCookStatStatus.PENDING; // 배치가 끝까지 성공해야 ACTIVE가 된다
    }
}
