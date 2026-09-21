package pengyu.menu.presentation; // 본인의 패키지 경로에 맞게 수정

import com.kds.menu.grpc.MenuRequest;
import com.kds.menu.grpc.MenuResponse;
import com.kds.menu.grpc.MenuGrpcServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import pengyu.menu.Menu;
import pengyu.menu.MenuNotFoundException;
import pengyu.menu.MenuService;

@GrpcService // 핵심: 스프링 빈으로 등록되면서 동시에 gRPC 서버로 동작하게 만듦
@RequiredArgsConstructor
public class MenuGrpcServiceImpl extends MenuGrpcServiceGrpc.MenuGrpcServiceImplBase {

    private final MenuService menuService;

    @Override
    public void getMenuInfo(MenuRequest request, StreamObserver<MenuResponse> responseObserver) {
        long menuId = request.getMenuId();

        try {
            Menu menu = menuService.getMenu(menuId);

            MenuResponse response = MenuResponse.newBuilder()
                    .setMenuId(menu.getId())
                    .setMenuName(menu.getName())
                    .setPrice(menu.getPrice())
                    .setCookingTime(menu.getCookingTime())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (MenuNotFoundException e) {
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asRuntimeException()
            );
        }
    }
}