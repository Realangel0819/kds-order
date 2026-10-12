package pengyu.order.search;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders/search")
@RequiredArgsConstructor
public class OrderSearchController {

    private final OrderSearchService orderSearchService;

    // 메뉴 이름 키워드 검색 API (GET) - 예: /api/orders/search?keyword=치킨&status=COMPLETED&page=0&size=20
    @GetMapping
    public OrderSearchResponse search(@RequestParam String keyword,
                                      @RequestParam(required = false) String status,
                                      @RequestParam(defaultValue = "0") int page,
                                      @RequestParam(defaultValue = "20") int size) {
        return orderSearchService.searchByMenuName(keyword, status, page, size);
    }
}
