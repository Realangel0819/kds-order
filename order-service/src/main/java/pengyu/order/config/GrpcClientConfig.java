package pengyu.order.config;

import com.kds.menu.grpc.MenuGrpcServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GrpcClientConfig {

    @Value("${grpc.client.menu-service.host}")
    private String host;

    @Value("${grpc.client.menu-service.port}")
    private int port;

    @Bean(destroyMethod = "shutdown")
    public ManagedChannel menuServiceChannel() {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    // 주의: 여기서 withDeadlineAfter(...)를 걸어서 빈으로 등록하면
    // "빈이 생성되는 시점" 기준으로 데드라인이 고정돼버립니다.
    // 그러면 앱 기동 후 2초가 지난 이후의 모든 호출은 요청을 보내보기도 전에 곧바로 타임아웃 납니다.
    // 그래서 데드라인은 여기서 걸지 않고, 실제 호출부(OrderService)에서 매번 새로 설정합니다.
    @Bean
    public MenuGrpcServiceGrpc.MenuGrpcServiceBlockingStub menuServiceBlockingStub(ManagedChannel menuServiceChannel) {
        return MenuGrpcServiceGrpc.newBlockingStub(menuServiceChannel);
    }
}
