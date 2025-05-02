package ch.fdsgn.balance_service.event;

import java.math.BigDecimal;
import java.time.Instant;

public record FundsDepositedEvent(
        String accountId,
        BigDecimal amount,
        Instant timestamp
) {
    public FundsDepositedEvent(String accountId, BigDecimal amount) {
        this(accountId, amount, Instant.now());
    }
} 