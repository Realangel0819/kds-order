package pengyu.order.batch;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 같은 주문 흐름을 30초 / 1분 / 2분 윈도우에 각각 흘려보내고 결과를 비교한다.
 * 실제로 기다리지 않고 MutableClock으로 1초씩 시간을 흘린다.
 */
class BatchWindowComparisonTest {

    private static final int THRESHOLD = 5;
    private static final int SIMULATION_SECONDS = 600; // 10분 (30/60/120으로 모두 나누어 떨어짐)
    private static final long SEED = 42L;               // 매번 같은 주문 흐름이 나오도록 고정
    private static final Instant START = Instant.parse("2026-10-01T12:00:00Z");

    private static final long POPULAR_MENU = 1L;   // 인기 메뉴
    private static final long NORMAL_MENU = 2L;    // 보통 메뉴
    private static final long UNPOPULAR_MENU = 3L; // 비인기 메뉴

    // 메뉴별 "1초 안에 주문이 들어올 확률"
    private static final Map<Long, Double> ORDER_PROBABILITY_PER_SECOND = Map.of(
            POPULAR_MENU, 0.50,
            NORMAL_MENU, 0.15,
            UNPOPULAR_MENU, 0.03);

    private record SimulatedOrder(int second, long menuId, int quantity) {
    }

    private record WindowResult(int windowSeconds, List<KdsBatch> batches) {

        long sendCount() {
            return batches.size();
        }

        long count(FlushTrigger trigger) {
            return batches.stream().filter(b -> b.trigger() == trigger).count();
        }

        int totalQuantity() {
            return batches.stream().mapToInt(KdsBatch::quantity).sum();
        }

        double avgBatchSize() {
            return batches.stream().mapToInt(KdsBatch::quantity).average().orElse(0);
        }

        double avgMaxWaitSeconds() {
            return batches.stream().mapToLong(b -> b.maxWait().toSeconds()).average().orElse(0);
        }

        long maxWaitSeconds() {
            return batches.stream().mapToLong(b -> b.maxWait().toSeconds()).max().orElse(0);
        }

        double avgMaxWaitSecondsOf(long menuId) {
            return batches.stream().filter(b -> b.menuId() == menuId)
                    .mapToLong(b -> b.maxWait().toSeconds()).average().orElse(0);
        }
    }

    @Test
    @DisplayName("30초 / 1분 / 2분 윈도우별로 KDS 전송 횟수, 묶음 크기, 대기 시간을 비교한다")
    void compareWindowSizes() {
        List<SimulatedOrder> orders = generateOrders();
        int orderedQuantity = orders.stream().mapToInt(SimulatedOrder::quantity).sum();

        List<WindowResult> results = List.of(
                simulate(orders, 30),
                simulate(orders, 60),
                simulate(orders, 120));

        printReport(orders, orderedQuantity, results);

        for (WindowResult result : results) {
            // 1. 어떤 윈도우든 들어온 수량은 전부 KDS로 나가야 한다 (유실 없음)
            assertThat(result.totalQuantity()).isEqualTo(orderedQuantity);
            // 2. 고정 윈도우가 매번 바구니를 비우므로 어떤 주문도 윈도우보다 오래 기다리지 않는다
            assertThat(result.maxWaitSeconds()).isLessThanOrEqualTo(result.windowSeconds());
            // 3. Size 배치는 임계치 이상, Time 배치는 임계치 미만
            assertThat(result.batches()).allSatisfy(b -> {
                if (b.trigger() == FlushTrigger.SIZE) {
                    assertThat(b.quantity()).isGreaterThanOrEqualTo(THRESHOLD);
                } else {
                    assertThat(b.quantity()).isLessThan(THRESHOLD);
                }
            });
        }

        // 4. 윈도우가 길어질수록 더 많이 모아서 보낸다 -> 전송 횟수 감소, 묶음 크기 증가, 대기 시간 증가
        WindowResult w30 = results.get(0);
        WindowResult w60 = results.get(1);
        WindowResult w120 = results.get(2);
        assertThat(w30.sendCount()).isGreaterThanOrEqualTo(w60.sendCount());
        assertThat(w60.sendCount()).isGreaterThanOrEqualTo(w120.sendCount());
        assertThat(w30.avgBatchSize()).isLessThanOrEqualTo(w60.avgBatchSize());
        assertThat(w60.avgBatchSize()).isLessThanOrEqualTo(w120.avgBatchSize());
        assertThat(w30.avgMaxWaitSecondsOf(UNPOPULAR_MENU)).isLessThan(w120.avgMaxWaitSecondsOf(UNPOPULAR_MENU));
    }

    private List<SimulatedOrder> generateOrders() {
        Random random = new Random(SEED);
        List<SimulatedOrder> orders = new ArrayList<>();
        for (int second = 0; second < SIMULATION_SECONDS; second++) {
            for (long menuId : List.of(POPULAR_MENU, NORMAL_MENU, UNPOPULAR_MENU)) {
                if (random.nextDouble() < ORDER_PROBABILITY_PER_SECOND.get(menuId)) {
                    orders.add(new SimulatedOrder(second, menuId, 1 + random.nextInt(2))); // 1~2개
                }
            }
        }
        return orders;
    }

    private WindowResult simulate(List<SimulatedOrder> orders, int windowSeconds) {
        MutableClock clock = new MutableClock(START);
        RecordingKdsSender sender = new RecordingKdsSender();
        BatchGroupingService service = new BatchGroupingService(sender, (groupId, orderIds) -> { }, clock, THRESHOLD);

        int index = 0;
        for (int second = 0; second <= SIMULATION_SECONDS; second++) {
            clock.setTo(START.plus(Duration.ofSeconds(second)));

            while (index < orders.size() && orders.get(index).second() == second) {
                SimulatedOrder order = orders.get(index++);
                service.addOrder((long) index, order.menuId(), order.quantity()); // index를 주문 ID로 사용
            }

            // @Scheduled(fixedRate) 역할: 윈도우 경계마다 자투리를 비운다
            if (second > 0 && second % windowSeconds == 0) {
                service.flushTimeWindowBatch();
            }
        }
        return new WindowResult(windowSeconds, sender.sent());
    }

    private void printReport(List<SimulatedOrder> orders, int orderedQuantity, List<WindowResult> results) {
        System.out.println("==================================================================================");
        System.out.printf("시뮬레이션: %d초 / 주문 %d건 / 총 수량 %d개 / 임계치 %d개%n",
                SIMULATION_SECONDS, orders.size(), orderedQuantity, THRESHOLD);
        System.out.println("----------------------------------------------------------------------------------");
        System.out.println("윈도우 | KDS 전송 | SIZE | TIME | 평균 묶음 | 평균 최대대기 | 최대 대기 | 비인기메뉴 평균대기");
        for (WindowResult r : results) {
            System.out.printf("%5ds | %7d회 | %4d | %4d | %7.2f개 | %11.1f초 | %7d초 | %15.1f초%n",
                    r.windowSeconds(), r.sendCount(), r.count(FlushTrigger.SIZE), r.count(FlushTrigger.TIME),
                    r.avgBatchSize(), r.avgMaxWaitSeconds(), r.maxWaitSeconds(),
                    r.avgMaxWaitSecondsOf(UNPOPULAR_MENU));
        }
        System.out.printf("(배치 없이 1건씩 보냈다면 KDS 전송 %d회)%n", orders.size());
        System.out.println("==================================================================================");
    }
}
