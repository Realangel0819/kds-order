package pengyu.order.stat;

/**
 * 일일 조리 통계 행의 상태. 조회 API는 ACTIVE만 본다.
 */
public enum DailyCookStatStatus {
    PENDING,  // 배치가 막 적재한 행. Job이 끝까지 성공하기 전까지는 조회되지 않는다
    ACTIVE,   // 현재 유효한 통계 (기준일자당 한 실행분만 ACTIVE)
    INACTIVE  // 재처리로 대체된 과거 통계 (이력 보관용)
}
