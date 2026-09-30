package pengyu.order.lock;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * @DistributedLock가 붙은 메서드를 가로채 Redisson 락으로 감쌉니다.
 * @Transactional보다 먼저 실행되고 나중에 풀려야(= 더 바깥을 감싸야) 트랜잭션 커밋 전에
 * 락이 풀려서 다른 스레드가 아직 커밋되지 않은 상태를 읽는 문제를 막을 수 있어 Order를 최상위로 둔다.
 */
@Slf4j
@Aspect
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class DistributedLockAspect {

    private static final String LOCK_KEY_PREFIX = "LOCK:";

    private final RedissonClient redissonClient;
    private final ExpressionParser parser = new SpelExpressionParser();
    private final DefaultParameterNameDiscoverer parameterNameDiscoverer = new DefaultParameterNameDiscoverer();

    @Around("@annotation(distributedLock)")
    public Object lock(ProceedingJoinPoint joinPoint, DistributedLock distributedLock) throws Throwable {
        String lockKey = LOCK_KEY_PREFIX + parseKey(joinPoint, distributedLock.key());
        RLock rLock = redissonClient.getLock(lockKey);

        boolean isLocked = false;
        try {
            isLocked = rLock.tryLock(distributedLock.waitTime(), distributedLock.leaseTime(), distributedLock.timeUnit());
            if (!isLocked) {
                throw new IllegalStateException("[" + lockKey + "] 락 획득 실패 (요청 폭주)");
            }

            log.info("[분산락 획득] key={}, thread={}", lockKey, Thread.currentThread().getName());
            return joinPoint.proceed();
        } finally {
            if (isLocked && rLock.isHeldByCurrentThread()) {
                rLock.unlock();
                log.info("[분산락 해제] key={}, thread={}", lockKey, Thread.currentThread().getName());
            }
        }
    }

    // 어노테이션의 key(SpEL)를 실제 메서드 인자 값으로 치환해 평가한다. (파라미터명은 -parameters/디버그 정보로 조회)
    private String parseKey(ProceedingJoinPoint joinPoint, String keyExpression) {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Object[] args = joinPoint.getArgs();
        String[] parameterNames = parameterNameDiscoverer.getParameterNames(method);

        StandardEvaluationContext context = new StandardEvaluationContext();
        for (int i = 0; i < parameterNames.length; i++) {
            context.setVariable(parameterNames[i], args[i]);
        }

        Expression expression = parser.parseExpression(keyExpression);
        return expression.getValue(context, String.class);
    }
}
