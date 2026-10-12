package pengyu.order;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "orders", // SQL 예약어 'order'와 겹치는 것을 방지하기 위해 테이블명을 'orders'로 지정
        indexes = {
                // ES 재색인 배치가 "어제 완료된 주문"을 범위 조회할 때 타는 인덱스
                @Index(name = "idx_orders_completed_at", columnList = "completed_at")
        })
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long menuId; // 어떤 메뉴를 주문했는지 (FK 대용으로 우선 ID만 보관)

    private int quantity; // 주문 수량

    // 주문 시점의 메뉴 이름/단가 스냅샷 (나중에 메뉴 가격이 바뀌어도 이 주문의 결제 금액은 그대로여야 함)
    // 이 컬럼이 추가되기 전 주문은 null
    private String menuName;

    private Integer price;

    @Enumerated(EnumType.STRING)
    private OrderStatus status; // 주문 상태 (PENDING, COOKING 등)

    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt; // 일일 통계 배치가 "어제 주문"을 고르는 기준

    // ES 재색인 배치가 "어제 완료된 주문"을 고르는 기준
    // createdAt으로 고르면 자정 직전에 주문하고 다음날 완료된 건이 빠진다
    @Column(name = "completed_at")
    private LocalDateTime completedAt;

    @Column(name = "group_id")
    private String groupId; // KDS로 한 번에 보낸 묶음 ID (같은 값 = 한 번에 조리)

    // createdAt/completedAt/groupId는 테스트에서 "어제 주문", "묶인 주문"을 직접 만들 수 있도록 빌더로도 받는다
    @Builder
    public Order(Long menuId, int quantity, String menuName, Integer price, OrderStatus status,
                 LocalDateTime createdAt, LocalDateTime completedAt, String groupId) {
        this.menuId = menuId;
        this.quantity = quantity;
        this.menuName = menuName;
        this.price = price;
        this.status = status;
        this.createdAt = createdAt;
        this.completedAt = completedAt;
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
        // 이미 COMPLETED인 주문에 COMPLETED가 다시 와도 완료 시각은 처음 값을 유지한다
        if (newStatus == OrderStatus.COMPLETED && this.status != OrderStatus.COMPLETED) {
            this.completedAt = LocalDateTime.now();
        }
        this.status = newStatus;
    }
}
