package pengyu.order.stat;

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
 * DailyCookStatJob 스케줄 실행. 매일 새벽 "어제"를 집계한다. (수동 실행은 BatchController)
 */
@Slf4j
@Component
public class DailyCookStatJobRunner {

    private final JobLauncher jobLauncher;
    private final Job dailyCookStatJob;
    private final Clock clock;

    public DailyCookStatJobRunner(JobLauncher jobLauncher, Job dailyCookStatJob, Clock clock) {
        this.jobLauncher = jobLauncher;
        this.dailyCookStatJob = dailyCookStatJob;
        this.clock = clock;
    }

    // 자정 직후에는 전날 마지막 주문의 상태 변경(COMPLETED)이 아직 끝나지 않았을 수 있어 약간 여유를 둔다
    @Scheduled(cron = "${stat.daily-cook.cron:0 10 0 * * *}")
    public void runForYesterday() throws Exception {
        run(LocalDate.now(clock).minusDays(1));
    }

    public JobExecution run(LocalDate targetDate) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("targetDate", targetDate.toString())
                // targetDate만으로 JobInstance를 식별하면 같은 날짜를 다시 돌릴 때
                // JobInstanceAlreadyCompleteException이 난다. 실행 시각을 함께 넣어 재처리를 허용한다.
                // (재처리해도 이전 ACTIVE는 INACTIVE로 내려가므로 ACTIVE 통계는 중복되지 않는다)
                .addLong("time", clock.millis())
                .toJobParameters();

        JobExecution execution = jobLauncher.run(dailyCookStatJob, params);
        if (execution.getStatus() != BatchStatus.COMPLETED) {
            log.error("[DailyCookStat] {} 집계 실패 - status={}, exceptions={}",
                    targetDate, execution.getStatus(), execution.getAllFailureExceptions());
        }
        return execution;
    }
}
