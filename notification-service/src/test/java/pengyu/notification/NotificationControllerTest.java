package pengyu.notification;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext;
import org.springframework.boot.web.embedded.netty.NettyWebServer;
import org.springframework.http.MediaType;
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
}
