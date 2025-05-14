package ch.fdsgn.balance_service.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record FundsWithdrawnEvent(
        UUID eventId,
        String accountId,
        BigDecimal amount,
        Instant timestamp,
        String eventType
) {

    public FundsWithdrawnEvent(String accountId, BigDecimal amount) {
        this(UUID.randomUUID(), accountId, amount, Instant.now(), EventType.WITHDRAWAL.getValue());
    }

    public FundsWithdrawnEvent(UUID eventId, String accountId, BigDecimal amount, Instant timestamp) {
        this(eventId, accountId, amount, timestamp, EventType.WITHDRAWAL.getValue());
    }

    public FundsWithdrawnEvent(UUID eventId, String accountId, BigDecimal amount) {
        this(eventId, accountId, amount, Instant.now(), EventType.WITHDRAWAL.getValue());
    }
} 