package pengyu.notification

import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.spring.SpringExtension
import io.kotest.matchers.date.shouldBeAfter
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.web.embedded.netty.NettyWebServer
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.MediaType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.time.Duration
import java.time.LocalDateTime

// 랜덤 포트로 실제 서버를 띄워서 테스트한다 (MockMvc 대신 WebTestClient 사용)
// Kafka 리스너는 이 테스트에서 쓰지 않으므로 띄우지 않는다 (로컬 Kafka가 꺼져 있어도 테스트가 돌도록)
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["spring.kafka.listener.auto-startup=false"],
)
class NotificationControllerTest : FunSpec() {

    override fun extensions() = listOf(SpringExtension)

    @Autowired
    private lateinit var webTestClient: WebTestClient

    @Autowired
    private lateinit var context: ReactiveWebServerApplicationContext

    init {
        test("내장 서버가 Tomcat이 아니라 Netty로 떠야 한다") {
            context.webServer.shouldBeInstanceOf<NettyWebServer>()
        }

        test("GET /api/notifications/health 요청 시 Netty 구동 메시지를 반환한다") {
            webTestClient.get().uri("/api/notifications/health")
                .exchange()
                .expectStatus().isOk
                .expectBody<String>().isEqualTo("Notification Service is running on Netty")
        }

        test("GET /api/notifications/stream 은 SSE로 1초마다 NotificationMessage(JSON)를 끊김 없이 흘려보낸다") {
            val startedAt = LocalDateTime.now()
            val body = webTestClient.get().uri("/api/notifications/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(NotificationMessage::class.java) // data의 JSON을 다시 객체로 역직렬화
                .responseBody

            // 끝나지 않는 스트림이므로 앞의 3개만 확인하고 구독을 취소한다
            // timestamp는 매번 달라지므로 내용은 menuId/message만 비교하고, 시간은 순서만 확인한다
            StepVerifier.create(body)
                .assertNext { msg ->
                    msg.menuId shouldBe 1L
                    msg.message shouldBe "KDS 알림 #0"
                    msg.timestamp shouldBeAfter startedAt
                }
                .assertNext { msg -> msg.message shouldBe "KDS 알림 #1" }
                .assertNext { msg -> msg.message shouldBe "KDS 알림 #2" }
                .thenCancel()
                .verify(Duration.ofSeconds(10))
        }

        test("GET /api/v1/notifications/stream 은 1초마다 heartbeat 이벤트를 SSE로 보낸다") {
            val body = webTestClient.get().uri("/api/v1/notifications/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk
                .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
                .returnResult(object : ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .responseBody

            StepVerifier.create(body)
                .assertNext { event ->
                    event.id() shouldBe "0"
                    event.event() shouldBe "heartbeat"
                    event.data() shouldBe "연결 유지 중..."
                }
                .assertNext { event -> event.id() shouldBe "1" }
                .thenCancel()
                .verify(Duration.ofSeconds(10))
        }

        test("POST /api/v1/notifications/test 로 보낸 주문이 /orders 를 구독 중인 모든 KDS에게 전달된다") {
            val kds1 = subscribeOrders()
            val kds2 = subscribeOrders()

            StepVerifier.create(Flux.merge(kds1, kds2))
                .then { postOrder("주문 #1 아메리카노") }
                .expectNext("주문 #1 아메리카노", "주문 #1 아메리카노") // KDS 2대가 각각 1번씩 받는다
                .thenCancel()
                .verify(Duration.ofSeconds(10))
        }

        test("모든 KDS가 연결을 끊어도 Sink가 종료되지 않고, 새로 붙은 KDS가 알림을 받는다") {
            StepVerifier.create(subscribeOrders())
                .then { postOrder("주문 #2 라떼") }
                .expectNext("주문 #2 라떼")
                .thenCancel()
                .verify(Duration.ofSeconds(10))

            StepVerifier.create(subscribeOrders())
                .then { postOrder("주문 #3 모카") }
                .expectNext("주문 #3 모카")
                .thenCancel()
                .verify(Duration.ofSeconds(10))
        }
    }

    // exchange()는 응답 헤더를 받을 때까지 기다리므로, 리턴 시점엔 서버가 이미 Sink를 구독한 상태다.
    // 연결 확인용 주석(connected)은 걸러내고 알림 문구만 남긴다.
    private fun subscribeOrders(): Flux<String> =
        webTestClient.get().uri("/api/v1/notifications/orders")
            .accept(MediaType.TEXT_EVENT_STREAM)
            .exchange()
            .expectStatus().isOk
            .returnResult(object : ParameterizedTypeReference<ServerSentEvent<NotificationMessage>>() {})
            .responseBody
            .mapNotNull { event -> event.data()?.message }

    // timestamp는 생략해서 서버 기본값(받은 시각)으로 채워지는지도 함께 확인한다
    private fun postOrder(order: String) {
        webTestClient.post().uri("/api/v1/notifications/test")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(mapOf("menuId" to 1, "message" to order))
            .exchange()
            .expectStatus().isOk
    }
}
