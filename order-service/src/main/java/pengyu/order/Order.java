package pengyu.order;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

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

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt; // 일일 통계 배치가 "어제 주문"을 고르는 기준

    @Column(name = "group_id")
    private String groupId; // KDS로 한 번에 보낸 묶음 ID (같은 값 = 한 번에 조리)

    // createdAt/groupId는 테스트에서 "어제 주문", "묶인 주문"을 직접 만들 수 있도록 빌더로도 받는다
    @Builder
    public Order(Long menuId, int quantity, OrderStatus status, LocalDateTime createdAt, String groupId) {
        this.menuId = menuId;
        this.quantity = quantity;
        this.status = status;
        this.createdAt = createdAt;
        this.groupId = groupId;
    }

    // @CreationTimestamp는 직접 넣은 값도 덮어쓰기 때문에, 비어 있을 때만 채운다
    @PrePersist
    void prePersist() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }

    // 비즈니스 로직: 주문 상태 변경 (Dirty Checking 대상)
    public void changeStatus(OrderStatus newStatus) {
        this.status = newStatus;
    }
}