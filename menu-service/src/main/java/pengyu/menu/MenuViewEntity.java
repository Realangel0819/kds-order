package pengyu.menu;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@DynamoDbBean
@Getter
@Setter
@NoArgsConstructor
public class MenuViewEntity {

    // TODO(개선): Sort Key를 String(ISO-8601) 대신 Epoch Milliseconds(Long)로.
    //   1) 숫자 타입이 문자열보다 범위 검색(Range Query, 예: 어제~오늘)에서 더 빠름
    //   2) DynamoDB TTL은 초 단위 Epoch Time(숫자)만 지원 -> "N일 지난 로그 자동 삭제" 하려면 필수
    //   주의: 기존 아이템과 타입이 달라 스키마 호환 안 됨(마이그레이션 필요)
    private Long menuId;
    private String viewedAt; // ISO-8601
    private String userId;

    public MenuViewEntity(Long menuId, String viewedAt, String userId) {
        this.menuId = menuId;
        this.viewedAt = viewedAt;
        this.userId = userId;
    }

    // DynamoDB Enhanced Client는 getter에 어노테이션이 있어야 한다
    @DynamoDbPartitionKey
    public Long getMenuId() {
        return menuId;
    }

    @DynamoDbSortKey
    public String getViewedAt() {
        return viewedAt;
    }
}
