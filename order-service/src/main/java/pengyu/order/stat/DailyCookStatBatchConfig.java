package pengyu.order.stat;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import java.time.LocalDate;
import java.util.Map;

/**
 * 일일 조리 통계 배치 설정.
 * Boot 3(Batch 5)에서는 @EnableBatchProcessing을 붙이면 Boot의 배치 자동 설정이 꺼지므로 붙이지 않는다.
 */
@Slf4j
@Configuration
public class DailyCookStatBatchConfig {

    static final int CHUNK_SIZE = 100;

    // 메뉴별로 "주문 건수"와 "서로 다른 groupId 개수(= 실제 조리 횟수)"를 한 번에 집계한다.
    // ORDER BY가 없으면 페이지마다 정렬이 달라져 행이 누락/중복될 수 있다.
    static final String QUERY = """
            SELECT new pengyu.order.stat.CookStatRow(o.menuId, COUNT(o), COUNT(DISTINCT o.groupId))
            FROM Order o
            
            
            WHERE o.status = pengyu.order.OrderStatus.COMPLETED
              AND o.groupId IS NOT NULL
              AND o.createdAt >= :start AND o.createdAt < :end
            GROUP BY o.menuId
            ORDER BY o.menuId
            """;

    static final String JOB_NAME = "dailyCookStatJob";

    private final EntityManagerFactory entityManagerFactory;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public DailyCookStatBatchConfig(EntityManagerFactory entityManagerFactory,
                                    JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager) {
        this.entityManagerFactory = entityManagerFactory;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    // 1) 집계 후 PENDING으로 적재 -> 2) PENDING을 ACTIVE로 교체
    // "먼저 지우고 다시 쌓기"는 2단계 도중 실패하면 그날 통계가 통째로 사라진다.
    // PENDING으로 넣는 동안 조회 API는 기존 ACTIVE를 그대로 보고, 끝까지 성공했을 때만 한 트랜잭션에서 교체한다.
    @Bean
    public Job dailyCookStatJob(Step aggregateDailyCookStatStep, Step activateDailyCookStatStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(aggregateDailyCookStatStep)
                .next(activateDailyCookStatStep)
                .build();
    }

    @Bean
    public Step aggregateDailyCookStatStep(JpaPagingItemReader<CookStatRow> dailyCookStatReader,
                                           ItemProcessor<CookStatRow, DailyCookStat> dailyCookStatProcessor) {
        return new StepBuilder("aggregateDailyCookStatStep", jobRepository)
                .<CookStatRow, DailyCookStat>chunk(CHUNK_SIZE, transactionManager)
                .reader(dailyCookStatReader)
                .processor(dailyCookStatProcessor)
                .writer(dailyCookStatWriter())
                .build();
    }

    // 집계 행(CookStatRow) -> PENDING 통계 엔티티. 절감 횟수(saved)는 엔티티 생성자에서 계산한다.
    @Bean
    @StepScope
    public ItemProcessor<CookStatRow, DailyCookStat> dailyCookStatProcessor(
            @Value("#{jobParameters['targetDate']}") String targetDate,
            @Value("#{stepExecution.jobExecutionId}") Long jobExecutionId) {
        LocalDate statDate = LocalDate.parse(targetDate);
        return row -> DailyCookStat.builder()
                .statDate(statDate)
                .menuId(row.menuId())
                .totalOrderCount(Math.toIntExact(row.orderCount()))
                .actualCookCount(Math.toIntExact(row.cookCycles()))
                .jobExecutionId(jobExecutionId)
                .build();
    }

    // Tasklet은 Step 하나 = 트랜잭션 하나이므로, 아래 3개 쿼리 중 하나라도 실패하면 전부 롤백된다.
    // -> 기존 ACTIVE는 그대로 남고(유실 없음), 이번 PENDING은 다음 실행 때 deleteStalePending으로 정리된다.
    @Bean
    public Step activateDailyCookStatStep(Tasklet activateDailyCookStatTasklet) {
        return new StepBuilder("activateDailyCookStatStep", jobRepository)
                .tasklet(activateDailyCookStatTasklet, transactionManager)
                .build();
    }

    @Bean
    @StepScope
    public Tasklet activateDailyCookStatTasklet(DailyCookStatRepository repository,
                                                @Value("#{jobParameters['targetDate']}") String targetDate,
                                                @Value("#{stepExecution.jobExecutionId}") Long jobExecutionId) {
        LocalDate statDate = LocalDate.parse(targetDate);
        return (contribution, chunkContext) -> {
            int deactivated = repository.deactivate(statDate);
            int stale = repository.deleteStalePending(statDate, jobExecutionId);
            int activated = repository.activate(statDate, jobExecutionId);
            log.info("[DailyCookStat] {} 교체 완료 - 기존 ACTIVE {}건 -> INACTIVE, 실패 잔여 PENDING {}건 삭제, 신규 {}건 ACTIVE (jobExecutionId={})",
                    targetDate, deactivated, stale, activated, jobExecutionId);
            return RepeatStatus.FINISHED;
        };
    }

    // 항상 새 행이므로 merge(SELECT 후 INSERT) 대신 persist로 바로 INSERT
    @Bean
    public JpaItemWriter<DailyCookStat> dailyCookStatWriter() {
        return new JpaItemWriterBuilder<DailyCookStat>()
                .entityManagerFactory(entityManagerFactory)
                .usePersist(true)
                .build();
    }

    // @StepScope: Step이 실행될 때 빈이 만들어지므로 jobParameters['targetDate']를 런타임에 받을 수 있다
    @Bean
    @StepScope
    public JpaPagingItemReader<CookStatRow> dailyCookStatReader(
            @Value("#{jobParameters['targetDate']}") String targetDate) {
        return createReader(entityManagerFactory, LocalDate.parse(targetDate));
    }

    // 테스트에서도 같은 쿼리로 Reader를 만들 수 있도록 분리
    static JpaPagingItemReader<CookStatRow> createReader(EntityManagerFactory emf, LocalDate targetDate) {
        // DATE(o.createdAt) = :targetDate 처럼 컬럼을 함수로 감싸면 created_at 인덱스를 못 탄다.
        // 대신 [당일 00:00, 다음날 00:00) 반열린 구간으로 비교한다. (23:59:59.999 같은 경계 누락도 없음)
        Map<String, Object> params = Map.of(
                "start", targetDate.atStartOfDay(),
                "end", targetDate.plusDays(1).atStartOfDay());

        return new JpaPagingItemReaderBuilder<CookStatRow>()
                .name("dailyCookStatReader")
                .entityManagerFactory(emf)
                .queryString(QUERY)
                .parameterValues(params)
                .pageSize(CHUNK_SIZE) // 청크 크기와 맞춘다
                .build();
    }
}
