package pengyu.notification;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.concurrent.Queues;

import java.time.Duration;

@RestController
public class NotificationController {

    // 헬스 체크 API (GET)
    // MVC처럼 String을 바로 반환하지 않고 Mono(0~1개의 값을 비동기로 내보내는 스트림)로 감싸서 반환한다.
    // 요청 스레드(Netty 이벤트 루프)를 붙잡지 않고, 값이 준비되면 그때 응답이 나간다.
    @GetMapping("/api/notifications/health")
    public Mono<String> health() {
        return Mono.just("Notification Service is running on Netty");
    }

    // 실시간 알림 스트림 API (GET, SSE)
    // text/event-stream으로 응답하면 연결을 끊지 않고 데이터가 생길 때마다 "data:..." 형태로 계속 흘려보낸다.
    // Flux는 0~N개의 값을 시간에 걸쳐 내보내는 스트림 (Mono의 여러 개 버전)
    @GetMapping(value = "/api/notifications/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream() {
        return Flux.interval(Duration.ofSeconds(1))          // 1초마다 0, 1, 2... 방출
                .map(sequence -> "KDS 알림 #" + sequence);    // 숫자를 문자열로 변환
    }

    // 하트비트 SSE 스트림 API (GET, v1)
    // ServerSentEvent로 감싸면 data뿐 아니라 id, event 같은 SSE 필드도 직접 지정할 수 있다.
    // 프록시/로드밸런서는 일정 시간 데이터가 없으면 연결을 끊어버리므로, 주기적으로 하트비트를 보내 연결을 살려둔다.
    @GetMapping(value = "/api/v1/notifications/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> heartbeat() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence -> ServerSentEvent.<String>builder()
                        .id(String.valueOf(sequence))   // 재연결 시 클라이언트가 Last-Event-ID로 돌려보내는 값
                        .event("heartbeat")             // 클라이언트에서 addEventListener("heartbeat", ...)로 구독
                        .data("연결 유지 중...")
                        .build());
    }

    // 주문 알림 파이프 (Sink)
    // Sink는 외부에서 tryEmitNext()로 데이터를 밀어 넣으면, asFlux()로 구독 중인 쪽에 흘려보내는 "입구 달린 Flux"다.
    // multicast(): 구독자(KDS) 여럿이 같은 알림을 동시에 받는 브로드캐스트 방식
    // autoCancel=false: 기본값(true)이면 마지막 KDS가 연결을 끊는 순간 Sink가 종료돼서 이후 알림을 영영 못 보낸다.
    private final Sinks.Many<String> orderSink =
            Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false);

    // 주문 알림 SSE 구독 API (GET)
    // 연결을 열어두고, Sink에 주문이 들어올 때마다 실시간으로 내려보낸다.
    // 주의: Sink에 데이터가 없으면 첫 요소가 나갈 때까지 응답 헤더조차 전송되지 않는다.
    //       → 클라이언트는 연결 성공 여부를 모른 채 대기하게 되므로, 연결 직후 SSE 주석(": connected")을 하나 보내서 헤더를 즉시 내보낸다.
    //       (주석은 EventSource의 onmessage로 전달되지 않으므로 클라이언트 로직에 영향 없음)
    // merge 순서: Sink를 먼저 구독한 뒤 connected를 보내야, 클라이언트가 connected를 받은 시점엔 이미 알림을 받을 준비가 끝나 있다.
    @GetMapping(value = "/api/v1/notifications/orders", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<String>> orders() {
        Flux<ServerSentEvent<String>> orders = orderSink.asFlux()
                .map(order -> ServerSentEvent.<String>builder().data(order).build());
        return Flux.merge(orders, Flux.just(ServerSentEvent.<String>builder().comment("connected").build()));
    }

    // 테스트용 알림 발생 API (POST)
    // 요청 본문(주문 정보)을 Sink에 밀어 넣으면, 구독 중인 모든 KDS에게 전달된다.
    @PostMapping("/api/v1/notifications/test")
    public Mono<ResponseEntity<String>> publish(@RequestBody String order) {
        Sinks.EmitResult result = orderSink.tryEmitNext(order);

        // tryEmitNext는 예외 대신 결과 코드를 돌려준다.
        // 예: 두 요청이 동시에 emit하면 FAIL_NON_SERIALIZED로 실패할 수 있다.
        if (result.isFailure()) {
            return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body("알림 전송 실패: " + result));
        }
        return Mono.just(ResponseEntity.ok("알림 전송 완료: " + order));
    }
}
