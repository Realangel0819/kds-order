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
