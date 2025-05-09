package ch.fdsgn.balance_service.controller.dto;

import java.math.BigDecimal;

public record WithdrawRequestDto(
        BigDecimal amount
) {
    public WithdrawRequestDto {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Withdrawal amount must be provided and positive");
        }
    }
} 