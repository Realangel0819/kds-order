package pengyu.order.stat;

/**
 * Reader가 넘기는 item. 주문 1건이 아니라 "메뉴 하나의 하루치 집계" 1행이다.
 * Processor는 item끼리 값을 합칠 수 없으므로, 합산은 Reader 쿼리(GROUP BY)에서 끝낸다.
 *
 * @param orderCount 주문 건수
 * @param cookCycles 실제 조리 사이클 횟수 (= 서로 다른 groupId 개수)
 */
public record CookStatRow(Long menuId, long orderCount, long cookCycles) {
}
