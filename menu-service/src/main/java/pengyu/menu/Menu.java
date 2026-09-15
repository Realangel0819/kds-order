package pengyu.menu;

import jakarta.persistence.*;
import lombok.*;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Getter
public class Menu {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    private int price;

    private int cookingTime;

    @Enumerated(EnumType.STRING)
    private MenuStatus status;

    @Builder
    public Menu(String name, int price, int cookingTime, MenuStatus status) {
        this.name = name;
        this.price = price;
        this.cookingTime = cookingTime;
        this.status = status;
    }

    // 비즈니스 로직
    public void changePrice(int newPrice) {
        this.price = newPrice;
    }
}