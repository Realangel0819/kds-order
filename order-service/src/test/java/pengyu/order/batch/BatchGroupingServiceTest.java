package pengyu.order.batch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class BatchGroupingServiceTest {

    private static final int THRESHOLD = 5;

    private MutableClock clock;
    private RecordingKdsSender sender;
    private BatchGroupingService service;
    private final AtomicLong orderSeq = new AtomicLong(); // 주문 ID 발급용
    private final Map<String, List<Long>> recorded = new ConcurrentHashMap<>(); // groupId -> 주문 ID들

    @BeforeEach
    void setUp() {
        clock = new MutableClock(Instant.parse("2026-10-01T12:00:00Z"));
        sender = new RecordingKdsSender();
        service = new BatchGroupingService(sender, recorded::put, clock, THRESHOLD);
    }

    @Test
    @DisplayName("[Size 방어] 누적 수량이 임계치에 도달하면 윈도우를 기다리지 않고 즉시 전송한다")
    void sizeTrigger_flushesImmediately() {
        service.addOrder(orderSeq.incrementAndGet(), 1L, 2);
        service.addOrder(orderSeq.incrementAndGet(), 1L, 2);
        assertThat(sender.sent()).isEmpty(); // 4개: 아직 대기

        clock.advance(Duration.ofSeconds(3));
        service.addOrder(orderSeq.incrementAndGet(), 1L, 1); // 5개 도달

        assertThat(sender.sent()).singleElement().satisfies(batch -> {
            assertThat(batch.menuId()).isEqualTo(1L);
            assertThat(batch.quantity()).isEqualTo(5);
            assertThat(batch.trigger()).isEqualTo(FlushTrigger.SIZE);
            assertThat(batch.maxWait()).isEqualTo(Duration.ofSeconds(3)); // 첫 주문부터 잰 대기 시간
        });

        // 전송된 메뉴는 바구니에서 빠졌으므로 윈도우 플러시 때 다시 나가지 않는다
        service.flushTimeWindowBatch();
        assertThat(sender.sent()).hasSize(1);
    }

    @Test
    @DisplayName("[Time 방어] 임계치에 못 미친 자투리 주문은 윈도우 플러시 때 메뉴별로 한 번에 나간다")
    void timeTrigger_flushesLeftovers() {
        service.addOrder(orderSeq.incrementAndGet(), 1L, 3);
        service.addOrder(orderSeq.incrementAndGet(), 2L, 1);
        service.addOrder(orderSeq.incrementAndGet(), 2L, 1);
        assertThat(sender.sent()).isEmpty();

        clock.advance(Duration.ofSeconds(30));
        service.flushTimeWindowBatch();

        assertThat(sender.sent())
                .extracting(KdsBatch::menuId, KdsBatch::quantity, KdsBatch::trigger)
                .containsExactlyInAnyOrder(
                        tuple(1L, 3, FlushTrigger.TIME),
                        tuple(2L, 2, FlushTrigger.TIME));

        // 바구니가 비었으므로 다음 플러시에서는 아무것도 나가지 않는다
        service.flushTimeWindowBatch();
        assertThat(sender.sent()).hasSize(2);
    }

    @Test
    @DisplayName("임계치를 넘기는 주문이 들어오면 넘친 수량까지 한 묶음으로 전송한다")
    void overThreshold_sendsWholeAccumulation() {
        service.addOrder(orderSeq.incrementAndGet(), 1L, 4);
        service.addOrder(orderSeq.incrementAndGet(), 1L, 3); // 7개

        assertThat(sender.sent()).singleElement()
                .extracting(KdsBatch::quantity)
                .isEqualTo(7);
    }

    @Test
    @DisplayName("주문 추가와 윈도우 플러시가 동시에 일어나도 유실·중복 없이 전부 KDS로 전달된다")
    void concurrentAddAndFlush_noLostOrDuplicatedOrders() throws Exception {
        int threadCount = 1000;
        Long[] menuIds = {1L, 2L, 3L};

        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        // 주문이 쏟아지는 동안 스케줄러 역할을 하는 스레드가 계속 플러시를 시도한다
        AtomicBoolean running = new AtomicBoolean(true);
        Thread flusher = new Thread(() -> {
            while (running.get()) {
                service.flushTimeWindowBatch();
            }
        });
        flusher.start();

        for (int i = 0; i < threadCount; i++) {
            Long menuId = menuIds[i % menuIds.length];
            futures.add(executorService.submit(() -> {
                try {
                    startLatch.await();
                    service.addOrder(orderSeq.incrementAndGet(), menuId, 1);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        startLatch.countDown();
        doneLatch.await();
        running.set(false);
        flusher.join();
        executorService.shutdown();

        // submit()은 예외를 삼키므로 Future.get()으로 실패가 없었는지 확인한다
        for (Future<?> future : futures) {
            future.get();
        }

        service.flushTimeWindowBatch(); // 마지막 자투리까지 비운다

        // 들어온 수량 == 나간 수량 (유실도 중복도 없음)
        assertThat(sender.totalQuantity()).isEqualTo(threadCount);
        // 1개씩 들어왔으므로 Size 배치는 정확히 임계치(5개)에서 끊겨야 한다 (5를 넘기면 원자성이 깨진 것)
        assertThat(sender.sent())
                .filteredOn(batch -> batch.trigger() == FlushTrigger.SIZE)
                .allSatisfy(batch -> assertThat(batch.quantity()).isEqualTo(THRESHOLD));
        assertThat(sender.sent())
                .filteredOn(batch -> batch.trigger() == FlushTrigger.TIME)
                .allSatisfy(batch -> assertThat(batch.quantity()).isLessThan(THRESHOLD));
        // 모든 주문이 정확히 한 묶음에만 기록되어야 한다 (groupId 누락/중복 기록 없음)
        assertThat(recorded.values().stream().flatMap(List::stream))
                .doesNotHaveDuplicates()
                .hasSize(threadCount);
    }

    @Test
    @DisplayName("KDS로 나간 묶음의 주문들에 같은 groupId를 기록한다")
    void recordsSameGroupIdForBatchedOrders() {
        service.addOrder(10L, 1L, 2);
        service.addOrder(11L, 2L, 1); // 다른 메뉴는 별도 묶음
        service.addOrder(12L, 1L, 3); // 메뉴 1: 5개 도달 -> SIZE 전송

        KdsBatch sizeBatch = sender.sent().get(0);
        assertThat(sizeBatch.orderIds()).containsExactly(10L, 12L);
        assertThat(recorded).containsEntry(sizeBatch.groupId(), List.of(10L, 12L));

        service.flushTimeWindowBatch(); // 메뉴 2 자투리 -> TIME 전송
        KdsBatch timeBatch = sender.sent().get(1);
        assertThat(timeBatch.groupId()).isNotEqualTo(sizeBatch.groupId());
        assertThat(recorded).containsEntry(timeBatch.groupId(), List.of(11L));
    }

    @Test
    @DisplayName("groupId 기록이 실패해도 KDS 전송은 계속된다 (나머지 메뉴도 정상 전송)")
    void recorderFailure_doesNotStopSending() {
        BatchGroupingService failingService = new BatchGroupingService(sender, (groupId, orderIds) -> {
            throw new IllegalStateException("DB down");
        }, clock, THRESHOLD);

        failingService.addOrder(1L, 1L, 1);
        failingService.addOrder(2L, 2L, 1);
        failingService.flushTimeWindowBatch();

        assertThat(sender.sent()).extracting(KdsBatch::menuId).containsExactlyInAnyOrder(1L, 2L);
    }
}
