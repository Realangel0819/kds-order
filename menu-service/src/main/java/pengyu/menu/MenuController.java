package pengyu.menu;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController // 1. 내가 만든 API 창구(Controller)임을 선언
@RequestMapping("/api/menus") // 2. 기본 URL 주소 설정 (예: http://localhost:8080/api/menus)
@RequiredArgsConstructor
public class MenuController {

    private final MenuService menuService;

    // MenuController 클래스 안에 아래 메서드를 추가해 주세요.
    @PostMapping
    public String createMenu(@RequestParam String name, @RequestParam int price) {
        menuService.createMenu(name, price);
        return name + " 메뉴가 생성되었습니다.";
    }

    // 3. 가격 수정 API 엔드포인트
    // PATCH 메서드는 자원의 '일부'만 수정할 때 사용하는 HTTP 표준입니다.
    @PatchMapping("/{menuId}/price")
    public String updatePrice(
            @PathVariable Long menuId,
            @RequestParam int newPrice) { // URL에 파라미터로 새 가격을 받음

        // 4. 오전에 만든 Service의 메서드를 호출!
        menuService.updateMenuPrice(menuId, newPrice);

        return "메뉴 가격이 " + newPrice + "원으로 성공적으로 변경되었습니다.";
    }

    // 5. 메뉴 단건 조회 API
    @GetMapping("/{menuId}")
    public Menu getMenu(@PathVariable Long menuId) {
        return menuService.getMenu(menuId);
    }

    // 6. 메뉴 전체 조회 API
    @GetMapping
    public List<Menu> getMenus() {
        return menuService.getMenus();
    }

    // 7. 메뉴 삭제 API
    @DeleteMapping("/{menuId}")
    public String deleteMenu(@PathVariable Long menuId) {
        menuService.deleteMenu(menuId);
        return "메뉴 ID " + menuId + "가 삭제되었습니다.";
    }
}