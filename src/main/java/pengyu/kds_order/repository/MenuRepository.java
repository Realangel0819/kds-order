package pengyu.kds_order.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import pengyu.kds_order.domain.Menu;

public interface MenuRepository extends JpaRepository<Menu, Long> {
}