package pengyu.order;

import jakarta.persistence.*;
import lombok.*;

@Entity
@Table(name = "orders") // SQL 예약어 'order'와 겹치는 것을 방지하기 위해 테이블명을 'orders'로 지정
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long menuId; // 어떤 메뉴를 주문했는지 (FK 대용으로 우선 ID만 보관)

    private int quantity; // 주문 수량

    @Enumerated(EnumType.STRING)
    private OrderStatus status; // 주문 상태 (PENDING, COOKING 등)

    @Builder
    public Order(Long menuId, int quantity, OrderStatus status) {
        this.menuId = menuId;
        this.quantity = quantity;
        this.status = status;
    }

    // 비즈니스 로직: 주문 상태 변경 (Dirty Checking 대상)
    public void changeStatus(OrderStatus newStatus) {
        this.status = newStatus;
    }
}