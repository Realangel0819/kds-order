package pengyu.order;

import com.kds.menu.grpc.MenuGrpcServiceGrpc;
import com.kds.menu.grpc.MenuRequest;
import com.kds.menu.grpc.MenuResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest // 스프링 부트 컨테이너를 통째로 띄워서 실제 빈(Bean)들을 주입받아 테스트합니다.
class OrderServiceTimeoutTest {

    // [설정 상수 정의]
    private static final int MENU_SERVICE_RESPONSE_DELAY_SECONDS = 3; // 가짜 메뉴 서버가 일부러 3초 동안 응답을 지연시킬 시간
    private static final int CLIENT_TIMEOUT_SECONDS = 2; // 주문 서비스의 클라이언트가 기다려줄 수 있는 최대 시간 (타임아웃 한계)

    private static Server slowMenuServer; // 테스트 안에서 임시로 띄울 '가짜 gRPC 서버' 객체

    /**
     * [핵심 핵심 포인트 1: 동적 프로퍼티 주입]
     * 실제 Menu Service를 따로 켜기 귀찮고 복잡하므로, 테스트 코드 실행 직전에
     * '응답을 3초 동안 지연시키는 가짜 gRPC 서버'를 메모리에 띄우고,
     * Order Service가 그 가짜 서버를 바라보도록 설정 값(포트 번호)을 실시간으로 바꿔치기합니다.
     */
    @DynamicPropertySource
    static void pointClientAtSlowMenuServer(DynamicPropertyRegistry registry) throws IOException {

        // 1. port(0)을 주어 운영체제가 비어있는 랜덤한 빈 포트를 알아서 할당하도록 gRPC 서버를 빌드합니다.
        slowMenuServer = ServerBuilder.forPort(0)
                .addService(new MenuGrpcServiceGrpc.MenuGrpcServiceImplBase() {
                    @Override
                    public void getMenuInfo(MenuRequest request, StreamObserver responseObserver) {
                        try {
                            // 2. 클라이언트로부터 요청이 오면, 정상 응답을 바로 주지 않고 일부러 3초 동안 재웁니다 (지연 장애 재현)
                            TimeUnit.SECONDS.sleep(MENU_SERVICE_RESPONSE_DELAY_SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }

                        // 3. 3초가 지난 뒤 지연된 응답 객체를 반환합니다.
                        responseObserver.onNext(MenuResponse.newBuilder()
                                .setMenuId(request.getMenuId())
                                .setMenuName("지연 응답 메뉴")
                                .build());
                        responseObserver.onCompleted();
                    }
                })
                .build()
                .start(); // 4. 가짜 서버를 실제로 구동시킵니다!

        // 5. 방금 위에서 랜덤으로 할당된 포트 번호를 Order Service의 gRPC 클라이언트 설정에 강제로 덮어씌웁니다.
        registry.add("grpc.client.menu-service.port", () -> slowMenuServer.getPort());
        registry.add("grpc.client.menu-service.timeout-seconds", () -> CLIENT_TIMEOUT_SECONDS);
    }

    /**
     * [핵심 포인트 2: 깔끔한 자원 정리]
     * 테스트가 전부 끝난 후(`@AfterAll`), 메모리에 띄워뒀던 가짜 gRPC 서버를 안전하게 종료하여
     * 다음 테스트나 다른 포트에 간섭하지 않도록 깔끔하게 치웁니다.
     */
    @AfterAll
    static void shutdownSlowMenuServer() {
        if (slowMenuServer != null) {
            slowMenuServer.shutdownNow();
        }
    }

    @Autowired
    private OrderService orderService; // 테스트를 수행할 진짜 주문 서비스 객체

    @Test
    @DisplayName("Menu Service 응답 지연 시 Order Service가 타임아웃으로 방어한다")
    void timeoutProtectsOrderServiceThread() {
        // 테스트 시작 시점의 시간을 기록합니다 (소요 시간 측정용).
        long start = System.currentTimeMillis();

        /**
         * [핵심 포인트 3: 예외 발생 검증 (assertThrows)]
         * orderService.createOrder를 호출하면 내부적으로 gRPC 통신을 시도합니다.
         * - 서버는 3초 동안 자고 있고, 클라이언트는 2초 타임아웃이 걸려있으므로
         * - 정확히 2초 시점에 gRPC DEADLINE_EXCEEDED 예외가 터져야 정상입니다.
         * - OrderService가 이를 잡아서 IllegalStateException으로 던지는지 확인합니다.
         */
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> orderService.createOrder(1L, 2)
        );

        // 테스트가 끝난 시점의 시간을 재서 총 소요 시간을 구합니다.
        long elapsed = System.currentTimeMillis() - start;

        // 예외 메시지에 원하는 경고 문구가 담겨 있는지 검증합니다.
        assertThat(exception.getMessage()).contains("메뉴 서비스 응답 지연");

        /**
         * [핵심 포인트 4: 시간 검증 (스레드 풀 보호 증명)]
         * - Menu Service는 3초 동안 요청을 붙잡고 있겠지만,
         * - Order Service는 타임아웃(2초) 덕분에 3초를 다 기다리지 않고 2초 언저리에 즉시 탈출했어야 합니다.
         * - 따라서 소요 시간(elapsed)이 2초 이상이면서, 서버가 자는 시간인 3초보다는 확실히 적어야 합니다.
         */
        assertThat(elapsed)
                .isGreaterThanOrEqualTo(TimeUnit.SECONDS.toMillis(CLIENT_TIMEOUT_SECONDS))
                .isLessThan(TimeUnit.SECONDS.toMillis(MENU_SERVICE_RESPONSE_DELAY_SECONDS));
    }
}