package ch.fdsgn.balance_service.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FundsDepositedEvent(
        UUID eventId,
        String accountId,
        BigDecimal amount,
        Instant timestamp,
        String eventType
) {
    public FundsDepositedEvent(String accountId, BigDecimal amount) {
        this(UUID.randomUUID(), accountId, amount, Instant.now(), "DEPOSIT");
    }

    public FundsDepositedEvent(UUID eventId, String accountId, BigDecimal amount) {
        this(eventId, accountId, amount, Instant.now(), "DEPOSIT");
    }

    public FundsDepositedEvent(UUID eventId, String accountId, BigDecimal amount, Instant timestamp) {
        this(eventId, accountId, amount, timestamp, "DEPOSIT");
    }
} 