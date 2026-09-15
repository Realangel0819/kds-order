package pengyu.kds_order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pengyu.kds_order.domain.Order;

public interface OrderRepository extends JpaRepository<Order, Long> {

}