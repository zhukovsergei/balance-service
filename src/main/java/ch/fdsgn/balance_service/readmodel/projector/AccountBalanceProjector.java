package ch.fdsgn.balance_service.readmodel.projector;

import ch.fdsgn.balance_service.event.EventType;
import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.event.FundsWithdrawnEvent;
import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.readmodel.repository.AccountBalanceRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Service
public class AccountBalanceProjector {

    private static final Logger log = LoggerFactory.getLogger(AccountBalanceProjector.class);

    private final AccountBalanceRepository accountBalanceRepository;
    private final ObjectMapper objectMapper;

    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String LISTENER_GROUP_ID = "balance-projector-group";

    @Autowired
    public AccountBalanceProjector(AccountBalanceRepository accountBalanceRepository, ObjectMapper objectMapper) {
        this.accountBalanceRepository = accountBalanceRepository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = ACCOUNT_EVENTS_TOPIC, groupId = LISTENER_GROUP_ID)
    @Transactional
    public void handleAccountEvent(ConsumerRecord<String, Map> record) {
        Map<String, Object> eventData = record.value();
        if (eventData == null) {
            log.warn("eventData is null");
            return;
        }

        String eventType = (String) eventData.get("eventType");
        if (eventType == null) {
            log.warn("eventType is null");
            return;
        }

        try {
            UUID eventId = UUID.fromString((String) eventData.get("eventId"));
            String accountId = (String) eventData.get("accountId");
            
            Object amountObj = eventData.get("amount");
            BigDecimal amount;
            if (amountObj instanceof Number) {
                amount = new BigDecimal(amountObj.toString());
            } else if (amountObj instanceof String) {
                 amount = new BigDecimal((String) amountObj);
            } else if (amountObj instanceof BigDecimal) {
                 amount = (BigDecimal) amountObj;
            } else {
                 log.error("Unsupported amount type");
                 return; 
            }

            Object timestampObj = eventData.get("timestamp");
            Instant timestamp;
            if (timestampObj instanceof Number) {
                timestamp = Instant.ofEpochMilli(((Number) timestampObj).longValue());
            } else if (timestampObj instanceof String) {
                try {
                    timestamp = Instant.parse((String) timestampObj);
                } catch (Exception e) {
                    try {
                         timestamp = Instant.ofEpochMilli(Long.parseLong((String) timestampObj));
                    } catch (NumberFormatException nfe) {
                        log.error("Cannot parse timestamp string", e);
                        return;
                    }
                }
            } else if (timestampObj instanceof Instant) {
                timestamp = (Instant) timestampObj;
            } else {
                log.error("Unsupported amount type");
                 return;
            }

            if (EventType.DEPOSIT.getValue().equals(eventType)) {
                FundsDepositedEvent event = new FundsDepositedEvent(eventId, accountId, amount, timestamp, eventType);
                processFundsDeposited(event);
            } else if (EventType.WITHDRAWAL.getValue().equals(eventType)) {
                FundsWithdrawnEvent event = new FundsWithdrawnEvent(eventId, accountId, amount, timestamp, eventType);
                processFundsWithdrawn(event);
            } else {
                log.warn("unknown eventType: {}", eventType);
            }
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
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
} 