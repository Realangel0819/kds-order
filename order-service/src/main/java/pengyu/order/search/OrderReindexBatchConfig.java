package pengyu.order.search;

import jakarta.persistence.EntityManagerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import pengyu.order.Order;
import pengyu.order.event.OrderCompletedEvent;

import java.time.LocalDate;
import java.util.Map;

/**
 * ES 재색인 배치 (정합성 보정).
 * Kafka 경로(Retry/DLT)는 Consumer 쪽 실패만 막는다. DB 커밋 후 이벤트 발행 자체가 실패하면(앱 다운, Kafka 장애)
 * 메시지가 아예 없어서 재시도할 대상도 없다. 그래서 하루 한 번 DB(원본)를 기준으로 그날 완료된 주문을 ES에 다시 넣는다.
 * 문서 ID = 주문 ID라 이미 색인된 주문은 같은 내용으로 덮어써질 뿐이므로, 비교 없이 통째로 다시 넣어도 안전하다.
 */
@Slf4j
@Configuration
public class OrderReindexBatchConfig {

    static final int CHUNK_SIZE = 500; // 청크 하나 = ES Bulk 요청 하나

    // ORDER BY가 없으면 페이지마다 정렬이 달라져 행이 누락/중복될 수 있다.
    static final String QUERY = """
            SELECT o
            FROM Order o
            WHERE o.status = pengyu.order.OrderStatus.COMPLETED
              AND o.completedAt >= :start AND o.completedAt < :end
            ORDER BY o.id
            """;

    static final String JOB_NAME = "orderReindexJob";

    private final EntityManagerFactory entityManagerFactory;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;

    public OrderReindexBatchConfig(EntityManagerFactory entityManagerFactory,
                                   JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager) {
        this.entityManagerFactory = entityManagerFactory;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    @Bean
    public Job orderReindexJob(Step orderReindexStep) {
        return new JobBuilder(JOB_NAME, jobRepository)
                .start(orderReindexStep)
                .build();
    }

    @Bean
    public Step orderReindexStep(JpaPagingItemReader<Order> orderReindexReader,
                                 ItemWriter<OrderDocument> orderReindexWriter) {
        return new StepBuilder("orderReindexStep", jobRepository)
                .<Order, OrderDocument>chunk(CHUNK_SIZE, transactionManager)
                .reader(orderReindexReader)
                .processor(orderReindexProcessor())
                .writer(orderReindexWriter)
                .build();
    }

    // @StepScope: Step이 실행될 때 빈이 만들어지므로 jobParameters['targetDate']를 런타임에 받을 수 있다
    @Bean
    @StepScope
    public JpaPagingItemReader<Order> orderReindexReader(@Value("#{jobParameters['targetDate']}") String targetDate) {
        LocalDate date = LocalDate.parse(targetDate);
        // DATE(completed_at) = :date 처럼 컬럼을 함수로 감싸면 인덱스를 못 타므로 [당일 00:00, 다음날 00:00) 구간으로 비교한다
        return new JpaPagingItemReaderBuilder<Order>()
                .name("orderReindexReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString(QUERY)
                .parameterValues(Map.of(
                        "start", date.atStartOfDay(),
                        "end", date.plusDays(1).atStartOfDay()))
                .pageSize(CHUNK_SIZE) // 청크 크기와 맞춘다
                .build();
    }

    // Kafka 경로와 똑같은 변환을 거치게 해서, 실시간 색인과 재색인 결과가 달라지지 않게 한다
    @Bean
    public ItemProcessor<Order, OrderDocument> orderReindexProcessor() {
        return order -> OrderDocument.from(OrderCompletedEvent.from(order));
    }

    // saveAll은 ES Bulk API 한 번으로 청크 전체를 저장한다.
    // ES 쓰기는 DB 트랜잭션에 묶이지 않지만, 멱등이라 실패한 Job을 다시 돌려도 결과는 같다.
    @Bean
    public ItemWriter<OrderDocument> orderReindexWriter(OrderSearchRepository orderSearchRepository) {
        return chunk -> {
            orderSearchRepository.saveAll(chunk.getItems());
            log.info("[OrderReindex] ES 재색인 {}건", chunk.size());
        };
    }
}
