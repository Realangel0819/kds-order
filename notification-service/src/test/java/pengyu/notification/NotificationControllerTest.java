package pengyu.notification;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.boot.web.embedded.netty.NettyWebServer;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

// 랜덤 포트로 실제 서버를 띄워서 테스트한다 (MockMvc 대신 WebTestClient 사용)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class NotificationControllerTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ReactiveWebServerApplicationContext context;

    @Test
    @DisplayName("내장 서버가 Tomcat이 아니라 Netty로 떠야 한다")
    void runsOnNetty() {
        assertThat(context.getWebServer()).isInstanceOf(NettyWebServer.class);
    }

    @Test
    @DisplayName("GET /api/notifications/health 요청 시 Netty 구동 메시지를 반환한다")
    void health() {
        webTestClient.get().uri("/api/notifications/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("Notification Service is running on Netty");
    }

    @Test
    @DisplayName("GET /api/notifications/stream 은 SSE로 1초마다 알림을 끊김 없이 흘려보낸다")
    void stream() {
        Flux<String> body = webTestClient.get().uri("/api/notifications/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(String.class)
                .getResponseBody();

        // 끝나지 않는 스트림이므로 앞의 3개만 확인하고 구독을 취소한다
        StepVerifier.create(body)
                .expectNext("KDS 알림 #0")
                .expectNext("KDS 알림 #1")
                .expectNext("KDS 알림 #2")
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("GET /api/v1/notifications/stream 은 1초마다 heartbeat 이벤트를 SSE로 보낸다")
    void heartbeat() {
        Flux<ServerSentEvent<String>> body = webTestClient.get().uri("/api/v1/notifications/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .getResponseBody();

        StepVerifier.create(body)
                .assertNext(event -> {
                    assertThat(event.id()).isEqualTo("0");
                    assertThat(event.event()).isEqualTo("heartbeat");
                    assertThat(event.data()).isEqualTo("연결 유지 중...");
                })
                .assertNext(event -> assertThat(event.id()).isEqualTo("1"))
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("POST /api/v1/notifications/test 로 보낸 주문이 /orders 를 구독 중인 모든 KDS에게 전달된다")
    void broadcastOrder() {
        Flux<String> kds1 = subscribeOrders();
        Flux<String> kds2 = subscribeOrders();

        StepVerifier.create(Flux.merge(kds1, kds2))
                .then(() -> postOrder("주문 #1 아메리카노"))
                .expectNext("주문 #1 아메리카노", "주문 #1 아메리카노")   // KDS 2대가 각각 1번씩 받는다
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("모든 KDS가 연결을 끊어도 Sink가 종료되지 않고, 새로 붙은 KDS가 알림을 받는다")
    void sinkSurvivesAfterAllSubscribersLeave() {
        StepVerifier.create(subscribeOrders())
                .then(() -> postOrder("주문 #2 라떼"))
                .expectNext("주문 #2 라떼")
                .thenCancel()
                .verify(Duration.ofSeconds(10));

        StepVerifier.create(subscribeOrders())
                .then(() -> postOrder("주문 #3 모카"))
                .expectNext("주문 #3 모카")
                .thenCancel()
                .verify(Duration.ofSeconds(10));
    }

    // exchange()는 응답 헤더를 받을 때까지 기다리므로, 리턴 시점엔 서버가 이미 Sink를 구독한 상태다.
    // 연결 확인용 주석(connected)은 걸러내고 주문 데이터만 남긴다.
    private Flux<String> subscribeOrders() {
        return webTestClient.get().uri("/api/v1/notifications/orders")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .getResponseBody()
                .filter(event -> event.data() != null)
                .map(ServerSentEvent::data);
    }

    private void postOrder(String order) {
        webTestClient.post().uri("/api/v1/notifications/test")
                .contentType(MediaType.TEXT_PLAIN)
                .bodyValue(order)
                .exchange()
                .expectStatus().isOk();
    }
}
