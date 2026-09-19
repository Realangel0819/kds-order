package pengyu.menu;

import java.time.LocalDateTime;

public record MenuViewEvent(
        Long menuId,
        LocalDateTime viewedAt
) {
}