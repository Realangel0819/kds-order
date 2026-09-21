package pengyu.menu.presentation;

import com.kds.menu.grpc.MenuRequest;
import com.kds.menu.grpc.MenuResponse;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import pengyu.menu.Menu;
import pengyu.menu.MenuNotFoundException;
import pengyu.menu.MenuService;
import pengyu.menu.MenuStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MenuGrpcServiceImplTest {

    private final MenuService menuService = mock(MenuService.class);
    private final MenuGrpcServiceImpl grpcService = new MenuGrpcServiceImpl(menuService);

    @Test
    void 메뉴가_존재하면_정상_응답을_반환한다() {
        Menu menu = Menu.builder()
                .name("시그니처 수제 버거")
                .price(8500)
                .cookingTime(12)
                .status(MenuStatus.AVAILABLE)
                .build();
        ReflectionTestUtils.setField(menu, "id", 1L);
        when(menuService.getMenu(1L)).thenReturn(menu);

        MenuRequest request = MenuRequest.newBuilder().setMenuId(1L).build();
        List<MenuResponse> responses = new ArrayList<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        grpcService.getMenuInfo(request, new StreamObserver<MenuResponse>() {
            @Override
            public void onNext(MenuResponse value) {
                responses.add(value);
            }

            @Override
            public void onError(Throwable t) {
                fail("onError가 호출되면 안 된다: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                completed.set(true);
            }
        });

        assertThat(responses).hasSize(1);
        MenuResponse response = responses.get(0);
        assertThat(response.getMenuId()).isEqualTo(1L);
        assertThat(response.getMenuName()).isEqualTo("시그니처 수제 버거");
        assertThat(response.getPrice()).isEqualTo(8500);
        assertThat(response.getCookingTime()).isEqualTo(12);
        assertThat(completed.get()).isTrue();
    }

    @Test
    void 메뉴가_없으면_NOT_FOUND_에러를_반환한다() {
        when(menuService.getMenu(99L)).thenThrow(new MenuNotFoundException(99L));

        MenuRequest request = MenuRequest.newBuilder().setMenuId(99L).build();
        AtomicReference<Throwable> errorHolder = new AtomicReference<>();

        grpcService.getMenuInfo(request, new StreamObserver<MenuResponse>() {
            @Override
            public void onNext(MenuResponse value) {
                fail("onNext가 호출되면 안 된다");
            }

            @Override
            public void onError(Throwable t) {
                errorHolder.set(t);
            }

            @Override
            public void onCompleted() {
                fail("onCompleted가 호출되면 안 된다");
            }
        });

        assertThat(errorHolder.get()).isInstanceOf(StatusRuntimeException.class);
        StatusRuntimeException sre = (StatusRuntimeException) errorHolder.get();
        assertThat(sre.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
    }
}
