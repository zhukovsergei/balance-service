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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class EventReplayService {
    private static final Logger log = LoggerFactory.getLogger(EventReplayService.class);
    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String REPLAY_GROUP_ID_PREFIX = "balance-replay-group-";

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
        try (Consumer<String, Map> consumer = createReplayConsumer()) {
            consumer.subscribe(Collections.singletonList(ACCOUNT_EVENTS_TOPIC));
            accountBalanceRepository.deleteAll();

            int emptyPolls = 0;
            final int maxEmptyPolls = 3;

            while (emptyPolls < maxEmptyPolls) {
                ConsumerRecords<String, Map> records = consumer.poll(Duration.ofMillis(500));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;

                records.forEach(record -> {
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
                                    return;
                                }
                            }
                        } else if (timestampObj instanceof Instant) {
                            timestamp = (Instant) timestampObj;
                        }

                        if ("DEPOSIT".equals(eventType)) {
                            replayFundsDeposited(new FundsDepositedEvent(eventId, accountId, amount, timestamp));
                        } else if ("WITHDRAWAL".equals(eventType)) {
                            replayFundsWithdrawn(new FundsWithdrawnEvent(eventId, accountId, amount, timestamp));
                        }

                    } catch (Exception e) {
                        log.error("Error: {}", e.getMessage(), e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException("Error to replay events", e);
        }
    }

    private void replayFundsDeposited(FundsDepositedEvent event) {
        AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                .orElseGet(() -> {
                    return new AccountBalance(event.accountId(), event.amount(), event.eventId());
                });

        if (accountBalance.getId() != null && !accountBalance.getProcessedEventIds().contains(event.eventId())) {
            accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().add(event.amount()));
            accountBalance.getProcessedEventIds().add(event.eventId());
        } else if (accountBalance.getId() != null && accountBalance.getProcessedEventIds().contains(event.eventId())) {
            log.debug("EventId {} already processed", event.eventId());
        }
        accountBalanceRepository.save(accountBalance);
    }

    private void replayFundsWithdrawn(FundsWithdrawnEvent event) {
        AccountBalance accountBalance = accountBalanceRepository.findById(event.accountId())
                .orElse(null); 

        if (accountBalance == null) {
            log.error("accountBalance is null.");
            return; 
        }

        if (accountBalance.getProcessedEventIds().contains(event.eventId())) {
            log.error("already processed");
            return;
        }

        accountBalance.setCurrentBalance(accountBalance.getCurrentBalance().subtract(event.amount()));
        accountBalance.getProcessedEventIds().add(event.eventId());
        accountBalanceRepository.save(accountBalance);
    }

    private Consumer<String, Map> createReplayConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REPLAY_GROUP_ID_PREFIX + UUID.randomUUID().toString()); 
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, java.util.Map.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "java.util,java.lang,java.math"); 

        return new KafkaConsumer<>(props);
    }
} 