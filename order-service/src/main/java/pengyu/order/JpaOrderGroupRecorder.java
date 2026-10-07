package pengyu.order;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import pengyu.order.batch.OrderGroupRecorder;

import java.util.List;

@Component
@RequiredArgsConstructor
public class JpaOrderGroupRecorder implements OrderGroupRecorder {

    private final OrderRepository orderRepository;

    @Override
    @Transactional
    public void record(String groupId, List<Long> orderIds) {
        orderRepository.assignGroupId(groupId, orderIds);
    }
}
