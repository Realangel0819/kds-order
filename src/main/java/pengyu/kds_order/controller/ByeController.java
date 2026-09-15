package pengyu.kds_order.controller;


import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ByeController{

    @GetMapping("/bye")
    public String bye(){
        return "감사합니다.";
    }
}
