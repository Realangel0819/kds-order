package pengyu.order.batch;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * 테스트에서 시간을 직접 흘려보내기 위한 Clock (실제로 30초를 기다리지 않는다)
 */
class MutableClock extends Clock {

    private volatile Instant now;

    MutableClock(Instant start) { this.now = start; }

    void setTo(Instant instant) { this.now = instant; }          // 시각을 특정 값으로 설정

    void advance(Duration duration) { this.now = now.plus(duration); }  // 시간을 앞으로 감기

    @Override
    public Instant instant() { return now; }     // "지금 몇 시야?" → 우리가 정한 시각 반환

    @Override
    public ZoneId getZone() { return ZoneOffset.UTC; }

    @Override
    public Clock withZone(ZoneId zone) { return this; }
}
