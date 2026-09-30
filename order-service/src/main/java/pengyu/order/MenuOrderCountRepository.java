package pengyu.order;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface MenuOrderCountRepository extends JpaRepository<MenuOrderCount, Long> {

    Optional<MenuOrderCount> findByMenuId(Long menuId);
}
