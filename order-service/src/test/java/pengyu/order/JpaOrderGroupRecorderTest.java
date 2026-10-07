package pengyu.order;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.context.annotation.Import;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
@Import(JpaOrderGroupRecorder.class)
class JpaOrderGroupRecorderTest {

    @Autowired
    JpaOrderGroupRecorder recorder;

    @Autowired
    TestEntityManager em;

    @Test
    @DisplayName("지정한 주문들에만 groupId를 UPDATE 한다")
    void assignsGroupIdOnlyToGivenOrders() {
        Long a = em.persist(order()).getId();
        Long b = em.persist(order()).getId();
        Long other = em.persist(order()).getId();
        em.flush();

        recorder.record("g-1", List.of(a, b));
        em.clear(); // 벌크 UPDATE는 영속성 컨텍스트를 거치지 않으므로 비우고 다시 읽는다

        assertThat(em.find(Order.class, a).getGroupId()).isEqualTo("g-1");
        assertThat(em.find(Order.class, b).getGroupId()).isEqualTo("g-1");
        assertThat(em.find(Order.class, other).getGroupId()).isNull();
    }

    private Order order() {
        return Order.builder().menuId(1L).quantity(1).status(OrderStatus.PENDING).build();
    }
}
