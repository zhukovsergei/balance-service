package ch.fdsgn.balance_service.domain.aggregate;

import ch.fdsgn.balance_service.command.DepositFundsCommand;
import ch.fdsgn.balance_service.command.WithdrawFundsCommand;
import ch.fdsgn.balance_service.domain.exception.InsufficientFundsException;
import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.event.FundsWithdrawnEvent;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Getter
@ToString
@NoArgsConstructor
public class Account {

    private String accountId;
    private BigDecimal balance = BigDecimal.ZERO;
    private final transient List<Object> pendingEvents = new ArrayList<>();

    public Account(String accountId) {
        if (accountId == null || accountId.isBlank()) {
            throw new IllegalArgumentException("Account ID cannot be blank for creation");
        }
        this.accountId = accountId;
    }

    public void handle(DepositFundsCommand command) {
        if (this.accountId == null) {
            throw new IllegalStateException("Can not be null.");
        }
        if (!this.accountId.equals(command.accountId())) {
            throw new IllegalArgumentException("Wrong accountId");
        }

        FundsDepositedEvent event = new FundsDepositedEvent(
                command.accountId(),
                command.amount()
        );

        apply(event);
        pendingEvents.add(event);
    }

    public void handle(WithdrawFundsCommand command) {
        if (this.accountId == null) {
            throw new IllegalStateException("Can not be null.");
        }
        if (!this.accountId.equals(command.accountId())) {
        }
        if (this.balance.compareTo(command.amount()) < 0) {
            throw new InsufficientFundsException("Insufficient funds for account " + this.accountId );
        }

        FundsWithdrawnEvent event = new FundsWithdrawnEvent(
                command.accountId(),
                command.amount()
        );

        apply(event);
        pendingEvents.add(event);
    }

    public void apply(FundsDepositedEvent event) {
         if (this.accountId == null) {
         }
         if (this.accountId != null && !this.accountId.equals(event.accountId())) {
              throw new IllegalStateException("Attempting to apply event for account " + event.accountId() + " to account " + this.accountId);
         }
        this.balance = this.balance.add(event.amount());
    }

    public void apply(FundsWithdrawnEvent event) {
        if (this.accountId == null) {
        }
        if (this.accountId != null && !this.accountId.equals(event.accountId())) {
             throw new IllegalStateException("Attempting to apply FundsWithdrawnEvent for account " + event.accountId() + " to account " + this.accountId);
        }
        this.balance = this.balance.subtract(event.amount());
    }

    public List<Object> getAndClearPendingEvents() {
        List<Object> events = new ArrayList<>(pendingEvents);
        pendingEvents.clear();
        return events;
    }

    // TODO: Withdraw
    // TODO: AccountCreatedEvent
} 