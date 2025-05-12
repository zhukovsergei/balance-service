package ch.fdsgn.balance_service.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FundsDepositedEvent(
        UUID eventId,
        String accountId,
        BigDecimal amount,
        Instant timestamp
) {
    public FundsDepositedEvent(String accountId, BigDecimal amount) {
        this(UUID.randomUUID(), accountId, amount, Instant.now());
    }

    public FundsDepositedEvent(UUID eventId, String accountId, BigDecimal amount) {
        this(eventId, accountId, amount, Instant.now());
    }

    public FundsDepositedEvent(UUID eventId, String accountId, BigDecimal amount, Instant timestamp) {
        this.eventId = eventId;
        this.accountId = accountId;
        this.amount = amount;
        this.timestamp = timestamp;
    }
} 