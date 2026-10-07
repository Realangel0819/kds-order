package pengyu.order.batch;

import java.util.List;

/**
 * KDS로 함께 나간 주문들에 같은 groupId를 남긴다. (일일 조리 통계 배치가 "실제 조리 횟수"를 세는 기준)
 * BatchGroupingService가 JPA를 직접 알지 않도록 분리했다. (단위 테스트에서는 람다로 대체)
 */
public interface OrderGroupRecorder {

    void record(String groupId, List<Long> orderIds);
}
