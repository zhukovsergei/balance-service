package ch.fdsgn.balance_service.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {


    @GetMapping("/")
    public ResponseEntity<String> ping() {
        return new ResponseEntity<>("Service is up!", HttpStatus.OK);
    }

}
