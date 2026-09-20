package pengyu.menu;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

@Slf4j
@Repository
public class MenuViewRepository {

    private static final String TABLE_NAME = "menu_views";

    private final DynamoDbTable<MenuViewEntity> table;

    public MenuViewRepository(DynamoDbEnhancedClient enhancedClient) {
        this.table = enhancedClient.table(TABLE_NAME, TableSchema.fromBean(MenuViewEntity.class));
    }

    // 로컬 DynamoDB는 깡통 상태이므로 테이블이 없으면 생성
    @PostConstruct
    void createTableIfNotExists() {
        try {
            table.describeTable();
        } catch (ResourceNotFoundException e) {
            table.createTable();
            log.info("DynamoDB 테이블 생성 완료: {}", TABLE_NAME);
        } catch (Exception e) {
            // DynamoDB가 꺼져 있어도 앱은 뜨도록 함 (저장 시점에 실패)
            log.warn("DynamoDB 테이블 확인 실패: {}", e.getMessage());
        }
    }

    public void save(MenuViewEntity entity) {
        table.putItem(entity);
    }
}
