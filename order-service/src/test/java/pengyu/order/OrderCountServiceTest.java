package pengyu.order;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class OrderCountServiceTest {

    private static final Long MENU_ID = 1L;
    private static final int THREAD_COUNT = 100;

    @Autowired
    private OrderCountService orderCountService;

    @Autowired
    private MenuOrderCountRepository menuOrderCountRepository;

    @BeforeEach
    void setUp() {
        // 테스트마다 카운트를 0으로 초기화한다.
        menuOrderCountRepository.deleteAll();
        menuOrderCountRepository.save(MenuOrderCount.builder()
                .menuId(MENU_ID)
                .count(0)
                .build());
    }

    @Test
    @DisplayName("분산락 덕분에 100개의 스레드가 동시에 카운트를 증가시켜도 Lost Update 없이 정확히 반영된다")
    void increaseCount_withLock_preventsLostUpdate() throws InterruptedException {
        // ExecutorService로 스레드 풀을 만들고 CountDownLatch로 100개의 요청이 모두 끝날 때까지 기다린다.
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    orderCountService.increaseCount(MENU_ID);
                } finally {
                    latch.countDown();
                }
            }));
        }

        latch.await();
        executorService.shutdown();

        //[임시 디버그] submit()은 예외를 삼키므로 Future.get()으로 실제 실패 원인을 확인했었다.
        // (분산락 AOP 바인딩 버그와 waitTime 부족 문제를 여기서 찾아냈다. 필요하면 다시 주석 해제해서 쓸 것)
         for (Future<?> future : futures) {
             try {
                 future.get();
             } catch (ExecutionException e) {
                 e.getCause().printStackTrace();
             }
         }

        MenuOrderCount result = menuOrderCountRepository.findByMenuId(MENU_ID).orElseThrow();

        // 눈으로 확인하기 위한 출력문 추가
        System.out.println("=====================================");
        System.out.println("최종 저장된 주문 수: " + result.getCount());
        System.out.println("=====================================");
        // 분산락으로 메뉴 단위 직렬화가 되므로 100번 모두 정확히 반영된다.
        assertThat(result.getCount()).isEqualTo(THREAD_COUNT);
    }
}
