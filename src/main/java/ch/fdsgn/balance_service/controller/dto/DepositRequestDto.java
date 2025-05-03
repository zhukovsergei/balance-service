package ch.fdsgn.balance_service.controller.dto;

import java.math.BigDecimal;

public record DepositRequestDto(
        BigDecimal amount
) {

    public DepositRequestDto {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Deposit amount must be provided and positive");
        }
    }
} 