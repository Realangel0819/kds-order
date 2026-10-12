package pengyu.order.search;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class OrderReindexController {

    private final OrderReindexJobRunner orderReindexJobRunner;

    // ES 재색인 배치 수동 실행 (POST) - 예: /api/batch/order-reindex?targetDate=2026-10-11
    // ES 장애가 길게 이어졌던 날이나, DLT에 쌓인 날을 다시 맞출 때 쓴다
    @PostMapping("/order-reindex")
    public String runOrderReindex(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate targetDate)
            throws Exception {
        JobExecution execution = orderReindexJobRunner.run(targetDate);
        return targetDate + " 주문 ES 재색인 배치 실행 결과: " + execution.getStatus()
                + " (jobExecutionId=" + execution.getId() + ")";
    }
}
