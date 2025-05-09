package ch.fdsgn.balance_service.readmodel.projector;

import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.event.FundsWithdrawnEvent;
import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.readmodel.repository.AccountBalanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Service
public class AccountBalanceProjector {

    private static final Logger log = LoggerFactory.getLogger(AccountBalanceProjector.class);

    private final AccountBalanceRepository accountBalanceRepository;

    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String LISTENER_GROUP_ID = "balance-projector-group";

    @Autowired
    public AccountBalanceProjector(AccountBalanceRepository accountBalanceRepository) {
        this.accountBalanceRepository = accountBalanceRepository;
    }

    // Один листенер для всех событий из топика
    @KafkaListener(topics = ACCOUNT_EVENTS_TOPIC, groupId = LISTENER_GROUP_ID)
    @Transactional
    public void handleAccountEvent(ConsumerRecord<String, Object> record) {
        Object event = record.value();

        if (event instanceof FundsDepositedEvent depositedEvent) {
            processFundsDeposited(depositedEvent);
        } else if (event instanceof FundsWithdrawnEvent withdrawnEvent) {
            processFundsWithdrawn(withdrawnEvent);
        } else {
            log.warn("Received unknown event type from record.value(): {}", event != null ? event.getClass().getName() : "null");
        }
    }

    private void processFundsDeposited(FundsDepositedEvent event) {
        log.info("Processing FundsDepositedEvent for id={}", event.eventId());
        try {
            AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                    .orElse(null);

            if (accountBalance != null && accountBalance.getProcessedEventIds().contains(event.eventId())) {
                log.warn("Duplicate FundsDepositedEvent detected for eventId={}", event.eventId());
                return;
            }

            if (accountBalance == null) {
                accountBalance = new AccountBalance(
                        event.accountId(),
                        event.amount(),
                        event.eventId()
                );
                log.info("Creating new balance for accountId: {}", event.accountId());
            } else {
                log.info("Updating existing balance for accountId: {}", event.accountId());
                accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().add(event.amount()));
                accountBalance.getProcessedEventIds().add(event.eventId());
            }
            accountBalanceRepository.save(accountBalance);
            log.info("Successfully projected FundsDepositedEvent for accountId: {}", event.accountId());
        } catch (Exception e) {
            log.error("Failed to project FundsDepositedEvent: {}", e.getMessage(), e);
        }
    }

    private void processFundsWithdrawn(FundsWithdrawnEvent event) {
        log.info("Processing FundsWithdrawnEvent for id={}", event.eventId());

        try {
            AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                    .orElse(null);

            if (accountBalance != null && accountBalance.getProcessedEventIds().contains(event.eventId())) {
                log.warn("Duplicate FundsWithdrawnEvent detected for eventId={}", event.eventId());
                return;
            }

            if (accountBalance == null) {
                log.error("AccountBalance not found for accountId {}", event.accountId());
                return;
            } else {
                log.info("Updating existing balance for accountId: {} with subtracting {}", event.accountId(), event.amount());

                accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().subtract(event.amount()));
                accountBalance.getProcessedEventIds().add(event.eventId());
            }
            accountBalanceRepository.save(accountBalance);
        } catch (Exception e) {
            log.error("Failed to project FundsWithdrawnEvent: {}", e.getMessage(), e);

        }
    }
    // TODO: AccountCreatedEvent
} 