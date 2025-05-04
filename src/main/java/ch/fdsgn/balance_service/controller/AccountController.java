package ch.fdsgn.balance_service.controller;

import ch.fdsgn.balance_service.command.DepositFundsCommand;
import ch.fdsgn.balance_service.controller.dto.DepositRequestDto;
import ch.fdsgn.balance_service.controller.dto.AccountBalanceDto;
import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.service.AccountCommandService;
import ch.fdsgn.balance_service.service.AccountQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Optional;

@RestController
public class AccountController {

    private static final Logger log = LoggerFactory.getLogger(AccountController.class);

    private final AccountCommandService accountCommandService;
    private final AccountQueryService accountQueryService;

    @Autowired
    public AccountController(AccountCommandService accountCommandService, AccountQueryService accountQueryService) {
        this.accountCommandService = accountCommandService;
        this.accountQueryService = accountQueryService;
    }

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
            return ResponseEntity.badRequest().build();
        } catch (RuntimeException e) {
            log.error("Error processing deposit for account {}: {}", accountId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @GetMapping("/accounts/{accountId}/balance")
    public ResponseEntity<AccountBalanceDto> getBalance(@PathVariable String accountId) {
        log.info("Received balance query for account {}", accountId);

        Optional<AccountBalance> balanceOptional = accountQueryService.getAccountBalance(accountId);

        return balanceOptional
                .map(AccountBalanceDto::createFromEntity)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    // TODO: POST /accounts/{accountId}/withdraw
} 