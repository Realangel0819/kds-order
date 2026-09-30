package pengyu.order.lock;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * 메서드에 붙이면 key로 식별되는 자원에 대해 Redisson 분산락을 걸고 실행합니다.
 * key는 SpEL 표현식이며, 파라미터 이름을 그대로 변수로 참조할 수 있습니다. (예: "'MENU_ORDER_LOCK_' + #menuId")
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface DistributedLock {

    String key();

    TimeUnit timeUnit() default TimeUnit.MILLISECONDS;

    long waitTime() default 3000L;

    long leaseTime() default 3000L;
}
