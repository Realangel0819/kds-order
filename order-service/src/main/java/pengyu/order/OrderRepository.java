package pengyu.order;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface OrderRepository extends JpaRepository<Order, Long> {

    // 묶음 단위로 UPDATE 한 번에 처리한다 (주문마다 조회 후 Dirty Checking하면 쿼리가 N번 나감)
    @Modifying
    @Query("UPDATE Order o SET o.groupId = :groupId WHERE o.id IN :orderIds")
    int assignGroupId(@Param("groupId") String groupId, @Param("orderIds") List<Long> orderIds);
}
