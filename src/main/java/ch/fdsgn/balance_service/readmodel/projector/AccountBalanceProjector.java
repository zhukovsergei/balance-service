package ch.fdsgn.balance_service.readmodel.projector;

import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.readmodel.repository.AccountBalanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

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

    @KafkaListener(topics = ACCOUNT_EVENTS_TOPIC, groupId = LISTENER_GROUP_ID)
    @Transactional
    public void handleFundsDeposited(@Payload FundsDepositedEvent event) {
        try {
            AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                    .orElseGet(() -> {
                        log.info("Creating new balance record for accountId: {}", event.accountId());
                        return new AccountBalance(event.accountId(), event.amount());
                    });

            if (accountBalance.getLastUpdatedAt() != null) {
                 log.info("Updating existing balance for accountId: {}. Current: {}, Adding: {}",
                         event.accountId(), accountBalance.getCurrentBalance(), event.amount());
                 accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().add(event.amount()));
            }


            accountBalanceRepository.save(accountBalance);

        } catch (Exception e) {
            log.error("Failed to project event {}: {}", event, e.getMessage(), e);
        }
    }

    // TODO: FundsWithdrawnEvent, AccountCreatedEvent
} 