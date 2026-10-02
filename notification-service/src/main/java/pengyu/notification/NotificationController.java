package pengyu.notification;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    // 헬스 체크 API (GET)
    // MVC처럼 String을 바로 반환하지 않고 Mono(0~1개의 값을 비동기로 내보내는 스트림)로 감싸서 반환한다.
    // 요청 스레드(Netty 이벤트 루프)를 붙잡지 않고, 값이 준비되면 그때 응답이 나간다.
    @GetMapping("/health")
    public Mono<String> health() {
        return Mono.just("Notification Service is running on Netty");
    }

    // 실시간 알림 스트림 API (GET, SSE)
    // text/event-stream으로 응답하면 연결을 끊지 않고 데이터가 생길 때마다 "data:..." 형태로 계속 흘려보낸다.
    // Flux는 0~N개의 값을 시간에 걸쳐 내보내는 스트림 (Mono의 여러 개 버전)
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream() {
        return Flux.interval(Duration.ofSeconds(1))          // 1초마다 0, 1, 2... 방출
                .map(sequence -> "KDS 알림 #" + sequence);    // 숫자를 문자열로 변환
    }
}
