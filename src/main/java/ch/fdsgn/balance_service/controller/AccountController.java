package ch.fdsgn.balance_service.controller;

import ch.fdsgn.balance_service.command.DepositFundsCommand;
import ch.fdsgn.balance_service.controller.dto.DepositRequestDto;
import ch.fdsgn.balance_service.service.AccountCommandService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
public class AccountController {

    private static final Logger log = LoggerFactory.getLogger(AccountController.class);

    private final AccountCommandService accountCommandService;

    @Autowired
    public AccountController(AccountCommandService accountCommandService) {
        this.accountCommandService = accountCommandService;
    }

    // POST /accounts/{accountId}/deposit
    @PostMapping("/accounts/{accountId}/deposit")
    public ResponseEntity<Void> depositFunds(
            @PathVariable String accountId,
            @RequestBody DepositRequestDto depositRequest) {

        log.info("Received deposit request for account {}: {}", accountId, depositRequest);

        DepositFundsCommand command = new DepositFundsCommand(
                accountId,
                depositRequest.amount()
        );

        try {
            accountCommandService.handle(command);
            return ResponseEntity.accepted().build();
        } catch (IllegalArgumentException e) {
            log.warn("Invalid deposit request for account {}: {}", accountId, e.getMessage());
            return ResponseEntity.badRequest().build(); // 400 Bad Request
        } catch (RuntimeException e) {
            log.error("Error processing deposit for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build(); // 500 Internal Server Error
        }
    }

    // TODO: POST /accounts/{accountId}/withdraw
    // TODO: POST /accounts
    // TODO: GET /accounts/{accountId}/balance
} 