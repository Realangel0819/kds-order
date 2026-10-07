package pengyu.order.stat;

import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.List;

// 배치 수동 실행은 BatchController(POST /api/batch/cook-stat)에서 한다
@RestController
@RequestMapping("/api/stats/daily-cook")
@RequiredArgsConstructor
public class DailyCookStatController {

    private final DailyCookStatRepository dailyCookStatRepository;

    // 적재된 통계 조회 (GET) - 배치가 끝까지 성공한 ACTIVE 통계만 보여준다
    @GetMapping
    public List<DailyCookStat> getStats(@RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        return dailyCookStatRepository.findByStatDateAndStatusOrderByMenuId(date, DailyCookStatStatus.ACTIVE);
    }
}
