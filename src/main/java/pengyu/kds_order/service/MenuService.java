package pengyu.kds_order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pengyu.kds_order.domain.Menu;
import pengyu.kds_order.domain.MenuStatus;
import pengyu.kds_order.repository.MenuRepository;

import java.util.List;

@Service
@RequiredArgsConstructor //menurepository를 의존성주입(DI)
public class MenuService {

    private final MenuRepository menuRepository;

    // MenuService 클래스 안에 아래 메서드를 추가해 주세요.
    @Transactional
    public void createMenu(String name, int price) {
        Menu newMenu = Menu.builder()
                .name(name)
                .price(price)
                .status(MenuStatus.AVAILABLE)
                .build();

        menuRepository.save(newMenu); // 이건 새로운 객체니까 save() 필수!
    }
    @Transactional
    public void updateMenuPrice(Long menuId, int newPrice) {
        // 1. DB에서 변경할 메뉴를 찾아옵니다. (영속 상태로 만듦)
        Menu menu = menuRepository.findById(menuId)
                .orElseThrow(() -> new IllegalArgumentException("해당 메뉴가 없습니다. id=" + menuId));

        // 2. 객체의 가격만 쓱 바꿉니다.
        menu.changePrice(newPrice);

        // 3. 주의: menuRepository.save(menu); 를 쓰지 않았습니다!
    }

    @Transactional(readOnly = true)
    public Menu getMenu(Long menuId) {
        return menuRepository.findById(menuId)
                .orElseThrow(() -> new IllegalArgumentException("해당 메뉴가 없습니다. id=" + menuId));
    }

    @Transactional(readOnly = true)
    public List<Menu> getMenus() {
        return menuRepository.findAll();
    }

    @Transactional
    public void deleteMenu(Long menuId) {
        if (!menuRepository.existsById(menuId)) {
            throw new IllegalArgumentException("해당 메뉴가 없습니다. id=" + menuId);
        }
        menuRepository.deleteById(menuId);
    }
}