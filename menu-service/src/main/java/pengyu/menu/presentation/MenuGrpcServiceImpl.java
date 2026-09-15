package pengyu.menu.presentation; // 본인의 패키지 경로에 맞게 수정

import com.kds.menu.grpc.MenuRequest;
import com.kds.menu.grpc.MenuResponse;
import com.kds.menu.grpc.MenuGrpcServiceGrpc;
import io.grpc.stub.StreamObserver;
import net.devh.boot.grpc.server.service.GrpcService;

@GrpcService // 핵심: 스프링 빈으로 등록되면서 동시에 gRPC 서버로 동작하게 만듦
public class MenuGrpcServiceImpl extends MenuGrpcServiceGrpc.MenuGrpcServiceImplBase {

    // private final MenuService menuService; // 실제 비즈니스 로직을 처리하는 서비스 (JPA 연동용)

    @Override
    public void getMenuInfo(MenuRequest request, StreamObserver responseObserver) {
        // 1. 요청으로부터 menu_id 추출
        long menuId = request.getMenuId();
        System.out.println("gRPC 요청 수신 - menuId: " + menuId);

        // 2. [임시 테스트용 데이터] 실제 구현 시에는 menuService.findById(menuId) 등을 호출
        // 지금은 단독 검증을 위해 하드코딩된 응답을 만들어보자.
        MenuResponse response = MenuResponse.newBuilder()
                .setMenuId(menuId)
                .setMenuName("시그니처 수제 버거")
                .setPrice(8500)
                .setCookingTime(12) // 12분
                .build();

        // 3. 클라이언트에게 응답 전송 완료 알림
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}