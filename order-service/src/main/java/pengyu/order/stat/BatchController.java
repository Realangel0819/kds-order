package pengyu.order.stat;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchController {

    private final JobLauncher jobLauncher;
    private final Job dailyCookStatJob; // 파라미터 이름으로 dailyCookStatJob 빈이 주입된다

    // 일일 조리 통계 배치 수동 실행 (POST) - 예: /api/batch/cook-stat?targetDate=2026-10-05
    // LocalDate로 받아서 형식이 틀린 날짜(2026-13-01 등)는 배치를 띄우기 전에 400으로 막는다
    @PostMapping("/cook-stat")
    public String runCookStat(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate targetDate)
            throws Exception {
        JobParameters parameters = new JobParametersBuilder()
                .addString("targetDate", targetDate.toString())
                .addLong("time", System.currentTimeMillis()) // 중복 실행(재실행)을 허용하기 위한 식별자
                .toJobParameters();

        // 기본 JobLauncher는 동기 실행이라 Job이 끝난 뒤의 상태가 바로 돌아온다
        JobExecution execution = jobLauncher.run(dailyCookStatJob, parameters);
        return targetDate + " 일일 조리 통계 배치 실행 결과: " + execution.getStatus()
                + " (jobExecutionId=" + execution.getId() + ")";
    }
}
