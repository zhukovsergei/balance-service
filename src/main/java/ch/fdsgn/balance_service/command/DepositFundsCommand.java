package ch.fdsgn.balance_service.command;

import java.math.BigDecimal;

public record DepositFundsCommand(
        String accountId,
        BigDecimal amount
) {
    public DepositFundsCommand {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be blank");
        }
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Deposit amount must be positive");
        }
    }
} 