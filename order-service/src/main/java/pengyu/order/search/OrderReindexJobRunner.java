package pengyu.order.search;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.LocalDate;

/**
 * OrderReindexJob 스케줄 실행. 매일 새벽 "어제 완료된 주문"을 ES에 다시 넣는다. (수동 실행은 OrderReindexController)
 */
@Slf4j
@Component
public class OrderReindexJobRunner {

    private final JobLauncher jobLauncher;
    private final Job orderReindexJob;
    private final Clock clock;

    public OrderReindexJobRunner(JobLauncher jobLauncher, Job orderReindexJob, Clock clock) {
        this.jobLauncher = jobLauncher;
        this.orderReindexJob = orderReindexJob;
        this.clock = clock;
    }

    // 트래픽이 가장 적은 새벽에, 일일 통계 배치(00:10)와 겹치지 않게 돌린다
    @Scheduled(cron = "${search.reindex.cron:0 30 3 * * *}")
    public void runForYesterday() throws Exception {
        run(LocalDate.now(clock).minusDays(1));
    }

    public JobExecution run(LocalDate targetDate) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("targetDate", targetDate.toString())
                // 같은 날짜를 다시 돌릴 수 있도록 실행 시각을 함께 넣는다 (멱등이라 몇 번 돌려도 결과는 같음)
                .addLong("time", clock.millis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(orderReindexJob, params);
        if (execution.getStatus() != BatchStatus.COMPLETED) {
            log.error("[OrderReindex] {} 재색인 실패 - status={}, exceptions={}",
                    targetDate, execution.getStatus(), execution.getAllFailureExceptions());
        }
        return execution;
    }
}
