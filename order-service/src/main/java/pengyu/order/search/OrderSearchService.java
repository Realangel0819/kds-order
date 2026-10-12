package pengyu.order.search;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderSearchService {

    private final ElasticsearchOperations elasticsearchOperations;

    /**
     * 메뉴 이름 키워드로 주문 이력을 검색한다. (예: "치킨" -> "양념 치킨", "후라이드 치킨")
     * status가 있으면 그 상태와 정확히 일치하는 주문만 남긴다.
     */
    public OrderSearchResponse searchByMenuName(String keyword, String status, int page, int size) {
        NativeQuery query = NativeQuery.builder()
                .withQuery(q -> q.bool(b -> {
                    // must + match: menuName(Text)을 분석기로 쪼갠 토큰과 비교하고, 관련도 점수에 반영
                    b.must(m -> m.match(mt -> mt.field("menuName").query(keyword)));
                    if (status != null && !status.isBlank()) {
                        // filter + term: status(Keyword)는 정확히 일치만 보면 되고 점수 계산이 필요 없어 filter로 둔다 (ES가 캐싱 가능)
                        b.filter(f -> f.term(t -> t.field("status").value(status)));
                    }
                    return b;
                }))
                .withPageable(PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")))
                .build();

        SearchHits<OrderDocument> hits = elasticsearchOperations.search(query, OrderDocument.class);

        return new OrderSearchResponse(
                hits.getTotalHits(),
                hits.getSearchHits().stream()
                        .map(SearchHit::getContent)
                        .map(OrderSearchResponse.Item::from)
                        .toList());
    }
}
