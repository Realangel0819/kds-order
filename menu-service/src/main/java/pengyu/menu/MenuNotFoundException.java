package pengyu.menu;

public class MenuNotFoundException extends IllegalArgumentException {

    public MenuNotFoundException(Long menuId) {
        super("해당 메뉴가 없습니다. id=" + menuId);
    }
}
