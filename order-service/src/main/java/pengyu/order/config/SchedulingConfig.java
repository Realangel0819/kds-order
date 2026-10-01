package pengyu.order.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration
@EnableScheduling // 없으면 BatchGroupingService의 @Scheduled가 동작하지 않는다
public class SchedulingConfig {

    // 테스트에서 시간을 직접 조작할 수 있도록 Clock을 빈으로 주입받는다
    @Bean
    public Clock clock() {
        return Clock.systemDefaultZone();
    }
}
