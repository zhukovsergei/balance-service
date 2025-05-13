package ch.fdsgn.balance_service.service;

import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.event.FundsWithdrawnEvent;
import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.readmodel.repository.AccountBalanceRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class EventReplayService {
    private static final Logger log = LoggerFactory.getLogger(EventReplayService.class);
    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String REPLAY_GROUP_ID = "balance-replay-group";

    private final AccountBalanceRepository accountBalanceRepository;
    private final String bootstrapServers;

    public EventReplayService(
            AccountBalanceRepository accountBalanceRepository,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        this.accountBalanceRepository = accountBalanceRepository;
        this.bootstrapServers = bootstrapServers;
    }

    @Transactional
    public void replayEvents() {

        try (Consumer<String, Object> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(ACCOUNT_EVENTS_TOPIC));
            
            accountBalanceRepository.deleteAll();

            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }
                
                records.forEach(record -> {
                    Object event = record.value();
                    if (event instanceof FundsDepositedEvent depositedEvent) {
                        replayFundsDeposited(depositedEvent);
                    } else if (event instanceof FundsWithdrawnEvent withdrawnEvent) {
                        replayFundsWithdrawn(withdrawnEvent);
                    } else if (event instanceof java.util.Map map) {

                        try {
                            if (map.containsKey("amount") && map.containsKey("accountId") && map.containsKey("eventId")) {
                                String accountId = (String) map.get("accountId");
                                java.math.BigDecimal amount = new java.math.BigDecimal(map.get("amount").toString());
                                java.util.UUID eventId = java.util.UUID.fromString((String) map.get("eventId"));

                                String type = (String) map.get("__TypeId__");
                                if (type == null && record.headers() != null) {
                                    org.apache.kafka.common.header.Header header = record.headers().lastHeader("__TypeId__");
                                    if (header != null) {
                                        type = new String(header.value());
                                    }
                                }
                                if (type != null && type.contains("FundsDepositedEvent")) {
                                    replayFundsDeposited(new ch.fdsgn.balance_service.event.FundsDepositedEvent(eventId, accountId, amount));
                                } else if (type != null && type.contains("FundsWithdrawnEvent")) {
                                    replayFundsWithdrawn(new ch.fdsgn.balance_service.event.FundsWithdrawnEvent(eventId, accountId, amount));
                                } else {
                                    log.warn("Unknown event type in map: {}", type);
                                }
                            }
                        } catch (Exception ex) {
                            log.error("Failed: {}", map, ex);
                        }
                    } else {
                        log.warn("Unknown event: {}", event.getClass().getName());
                    }
                });
            }
            
        } catch (Exception e) {
            log.error("Error: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to replay events", e);
        }
    }

    private void replayFundsDeposited(FundsDepositedEvent event) {
        AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                .orElse(null);

        if (accountBalance == null) {

            accountBalance = new AccountBalance(event.accountId(), event.amount(), event.eventId());
            log.info("Creating new balance for accountId: {}", event.accountId());
        } else {

            if (accountBalance.getProcessedEventIds().contains(event.eventId())) {
                return;
            }
            log.info("Updating balance for accountId: {}", event.accountId());
            accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().add(event.amount()));
            accountBalance.getProcessedEventIds().add(event.eventId());
        }

        if (!accountBalance.getProcessedEventIds().contains(event.eventId())) {
            accountBalance.getProcessedEventIds().add(event.eventId());
        }
        accountBalanceRepository.save(accountBalance);
    }

    private void replayFundsWithdrawn(FundsWithdrawnEvent event) {
        AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                .orElse(null);
        
        if (accountBalance == null) {
            log.error("Account not found: {}", event.accountId());
            return;
        }
        
        if (accountBalance.getProcessedEventIds().contains(event.eventId())) {
            return;
        }
        
        accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().subtract(event.amount()));
        accountBalance.getProcessedEventIds().add(event.eventId());
        accountBalanceRepository.save(accountBalance);
    }

    private Consumer<String, Object> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REPLAY_GROUP_ID + "-" + java.util.UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ch.fdsgn.balance_service.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        
        return new KafkaConsumer<>(props);
    }
} 