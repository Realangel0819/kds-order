package pengyu.menu;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor //menurepository를 의존성주입(DI)
public class MenuService {

    private final MenuRepository menuRepository;
    private final MenuViewProducer menuViewProducer;

    // MenuService 클래스 안에 아래 메서드를 추가해 주세요.
    @Transactional
    public void createMenu(String name, int price, int cookingTime) {
        Menu newMenu = Menu.builder()
                .name(name)
                .price(price)
                .cookingTime(cookingTime)
                .status(MenuStatus.AVAILABLE)
                .build();

        menuRepository.save(newMenu); // 이건 새로운 객체니까 save() 필수!
    }
    @Transactional
    public void updateMenuPrice(Long menuId, int newPrice) {
        // 1. DB에서 변경할 메뉴를 찾아옵니다. (영속 상태로 만듦)
        Menu menu = menuRepository.findById(menuId)
                .orElseThrow(() -> new MenuNotFoundException(menuId));

        // 2. 객체의 가격만 쓱 바꿉니다.
        menu.changePrice(newPrice);

        // 3. 주의: menuRepository.save(menu); 를 쓰지 않았습니다!
    }

    @Transactional(readOnly = true)
    public Menu getMenu(Long menuId) {
        Menu menu = menuRepository.findById(menuId)
                .orElseThrow(() -> new MenuNotFoundException(menuId));

        // 조회에 성공했으므로 Kafka로 조회 이벤트 발행
        // 예외를 삼켜서 핵심 로직(메뉴 반환)을 보호합니다.
        try {
            menuViewProducer.sendMenuViewEvent(menuId);
        } catch (Exception e) {
            // 에러 로그만 남기고 정상 흐름으로 진행시킵니다.
            log.error("Kafka 이벤트 발행 실패 (메뉴 조회는 정상 처리됨): menuId={}", menuId, e);
        }
        return menu;
    }

    @Transactional(readOnly = true)
    public List<Menu> getMenus() {
        return menuRepository.findAll();
    }

    @Transactional
    public void deleteMenu(Long menuId) {
        if (!menuRepository.existsById(menuId)) {
            throw new MenuNotFoundException(menuId);
        }
        menuRepository.deleteById(menuId);
    }


}