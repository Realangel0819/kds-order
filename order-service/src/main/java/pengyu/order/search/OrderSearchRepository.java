package pengyu.order.search;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

// JpaRepository처럼 save/findById/deleteById 등을 제공한다. (키워드 검색 쿼리는 OrderSearchService에서 직접 만든다)
public interface OrderSearchRepository extends ElasticsearchRepository<OrderDocument, String> {
}
