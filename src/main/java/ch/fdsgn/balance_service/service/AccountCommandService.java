package ch.fdsgn.balance_service.service;

import ch.fdsgn.balance_service.command.DepositFundsCommand;
import ch.fdsgn.balance_service.command.WithdrawFundsCommand;
import ch.fdsgn.balance_service.domain.aggregate.Account;
import ch.fdsgn.balance_service.domain.exception.InsufficientFundsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class AccountCommandService {

    private static final Logger log = LoggerFactory.getLogger(AccountCommandService.class);

    private final EventPublisher eventPublisher;
    private final AccountEventStore accountEventStore;

    private final ConcurrentHashMap<String, Account> accountStore = new ConcurrentHashMap<>();


    @Autowired
    public AccountCommandService(EventPublisher eventPublisher, AccountEventStore accountEventStore) {
        this.eventPublisher = eventPublisher;
        this.accountEventStore = accountEventStore;
    }

    @Transactional
    public void handle(DepositFundsCommand command) {
        log.info("Handling command: {}", command);

        Account account = accountStore.computeIfAbsent(command.accountId(), accountId -> {
            log.info("Rebuilding depo for accountId {}", accountId);
            return accountEventStore.rebuildAccount(accountId);
        });

        try {
            account.handle(command);
        } catch (Exception e) {
            log.error("Error handling command {} for account {}: {}", command, command.accountId(), e.getMessage());
            throw new RuntimeException("Failed to handle command: " + e.getMessage(), e);
        }

        List<Object> pendingEvents = account.getAndClearPendingEvents();
        if (!pendingEvents.isEmpty()) {
            for (Object event : pendingEvents) {
                eventPublisher.publish(command.accountId(), event);
            }
            accountStore.put(command.accountId(), account);
        } else {
            log.warn("No events for deposit");
        }
    }

    @Transactional
    public void handle(WithdrawFundsCommand command) {
        Account account = accountStore.computeIfAbsent(command.accountId(), accountId -> {
            log.info("Rebuilding witdraw for accountId {}", accountId);
            return accountEventStore.rebuildAccount(accountId);
        });

        try {
            account.handle(command);
        } catch (InsufficientFundsException e) {
            log.warn("Insufficient funds for account {}: {}", command.accountId(), e.getMessage());
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to handle withdraw command: " + e.getMessage(), e);
        }

        List<Object> pendingEvents = account.getAndClearPendingEvents();
        if (!pendingEvents.isEmpty()) {
            for (Object event : pendingEvents) {
                eventPublisher.publish(command.accountId(), event);
            }
            accountStore.put(command.accountId(), account);
        } else {
            log.warn("No events for withdraw");
        }
    }

    // TODO: CreateAccountCommand
    // TODO: Event Sourcing Repository
}