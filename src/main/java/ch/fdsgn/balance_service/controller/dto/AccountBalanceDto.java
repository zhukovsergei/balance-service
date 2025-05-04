package ch.fdsgn.balance_service.controller.dto;

import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;

import java.math.BigDecimal;
import java.time.Instant;

public record AccountBalanceDto(
        String accountId,
        BigDecimal currentBalance,
        Instant lastUpdatedAt
) {

    public static AccountBalanceDto createFromEntity(AccountBalance entity) {
        return new AccountBalanceDto(
                entity.getId(),
                entity.getCurrentBalance(),
                entity.getLastUpdatedAt()
        );
    }
} 