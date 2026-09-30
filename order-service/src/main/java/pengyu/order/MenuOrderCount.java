package pengyu.order;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "menu_order_count")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class MenuOrderCount {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long menuId; // 어떤 메뉴의 누적 주문수인지

    private int count; // 누적 주문 횟수

    @Builder
    public MenuOrderCount(Long menuId, int count) {
        this.menuId = menuId;
        this.count = count;
    }

    // 비즈니스 로직: 카운트 1 증가 (Dirty Checking 대상)
    public void increase() {
        this.count += 1;
    }
}
