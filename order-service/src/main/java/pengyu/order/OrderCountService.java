package pengyu.order;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pengyu.order.lock.DistributedLock;

import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
public class OrderCountService {

    private final MenuOrderCountRepository menuOrderCountRepository;

    // 같은 메뉴로 몰리는 카운트 증가 요청을 메뉴 단위로 직렬화해 Lost Update를 막는다.
    // waitTime을 넉넉히 준 이유: 요청이 한 줄로 순서를 기다리는 구조라, 동시 요청이 몰릴수록
    // 뒤에 선 요청은 앞선 요청들이 끝날 때까지 오래 대기해야 하기 때문이다.
    @DistributedLock(key = "'MENU_ORDER_COUNT_LOCK_' + #menuId", waitTime = 10000, leaseTime = 3000, timeUnit = TimeUnit.MILLISECONDS)
    @Transactional
    public void increaseCount(Long menuId) {
        MenuOrderCount menuOrderCount = menuOrderCountRepository.findByMenuId(menuId)
                .orElseThrow(() -> new IllegalArgumentException("해당 메뉴의 카운트 정보가 없습니다. menuId=" + menuId));

        // 객체 상태만 바꾸면 트랜잭션 커밋 시점에 UPDATE 쿼리가 나감!
        menuOrderCount.increase();
    }
}
