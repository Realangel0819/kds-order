package pengyu.order.search;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import pengyu.order.event.OrderCompletedEvent;

import java.time.LocalDateTime;

/**
 * 검색/통계용으로 ES 'orders' 인덱스에 색인되는 주문 문서.
 * MySQL의 Order 엔티티와 분리해, 검색에 필요한 형태(메뉴 이름, 결제 금액 등 비정규화)로 들고 있는다.
 */
@Document(indexName = "orders")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class OrderDocument {

    @Id
    private String id; // 문서 고유 ID. 원본 주문 ID를 그대로 써서, 같은 이벤트가 다시 와도 덮어쓰기(멱등)가 되게 한다

    @Field(type = FieldType.Long)
    private Long orderId; // 원본 주문 ID (MySQL orders.id)

    @Field(type = FieldType.Keyword)
    private Long menuId; // 정확히 일치하는 값으로만 필터링하므로 Keyword

    // Text: 분석기가 "양념 치킨" -> ["양념", "치킨"]으로 쪼개 색인하므로 "치킨"으로도 검색된다
    // (기본 standard 분석기는 공백 기준이라 "양념치킨"처럼 붙어 있으면 안 잡힌다. 그땐 nori 플러그인 + analyzer = "nori")
    @Field(type = FieldType.Text)
    private String menuName;

    @Field(type = FieldType.Integer)
    private Integer totalPrice; // 총 결제 금액

    @Field(type = FieldType.Keyword)
    private String status; // "COMPLETED"처럼 정확히 일치하는 값만 검색하므로 Keyword

    // 포맷을 지정하지 않으면 LocalDateTime <-> ES date 변환에서 파싱 오류가 날 수 있어 명시한다 (예: 2026-10-12T13:45:30.123)
    @Field(type = FieldType.Date, format = DateFormat.date_hour_minute_second_millis)
    private LocalDateTime createdAt; // 주문 생성 일시

    @Builder
    public OrderDocument(Long orderId, Long menuId, String menuName, Integer totalPrice, String status, LocalDateTime createdAt) {
        this.id = String.valueOf(orderId);
        this.orderId = orderId;
        this.menuId = menuId;
        this.menuName = menuName;
        this.totalPrice = totalPrice;
        this.status = status;
        this.createdAt = createdAt;
    }

    // Kafka Consumer와 재색인 배치가 같은 변환을 쓰도록 한 곳에 둔다
    public static OrderDocument from(OrderCompletedEvent event) {
        return OrderDocument.builder()
                .orderId(event.orderId())
                .menuId(event.menuId())
                .menuName(event.menuName())
                .totalPrice(event.totalPrice())
                .status(event.status())
                .createdAt(event.createdAt())
                .build();
    }
}
