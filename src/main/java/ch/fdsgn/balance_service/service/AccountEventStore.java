package ch.fdsgn.balance_service.service;

import ch.fdsgn.balance_service.domain.aggregate.Account;
import ch.fdsgn.balance_service.event.EventType;
import ch.fdsgn.balance_service.event.FundsDepositedEvent;
import ch.fdsgn.balance_service.event.FundsWithdrawnEvent;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class AccountEventStore {
    private static final Logger log = LoggerFactory.getLogger(AccountEventStore.class);
    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String REBUILD_GROUP_ID_PREFIX = "account-rebuild-group-";

    private final String bootstrapServers;
    // private final ObjectMapper objectMapper;

    public AccountEventStore(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
                             /* ObjectMapper objectMapper */) {
        this.bootstrapServers = bootstrapServers;
        // this.objectMapper = objectMapper;
    }

    public Account rebuildAccount(String accountId) {
        Account account = new Account(accountId);

        try (Consumer<String, Map> consumer = createConsumerForRebuild()) {
            consumer.subscribe(Collections.singletonList(ACCOUNT_EVENTS_TOPIC));

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
                    if (!accountId.equals(record.key())) {
                        return;
                    }

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
                        // String recordAccountId = (String) eventData.get("accountId");

                        Object amountObj = eventData.get("amount");
                        BigDecimal amount;
                        if (amountObj instanceof Number) {
                            amount = new BigDecimal(amountObj.toString());
                        } else if (amountObj instanceof String) {
                            amount = new BigDecimal((String) amountObj);
                        } else if (amountObj instanceof BigDecimal) {
                            amount = (BigDecimal) amountObj;
                        } else {
                            log.error("Error");
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
                                    return;
                                }
                            }
                        } else if (timestampObj instanceof Instant) {
                            timestamp = (Instant) timestampObj;
                        } else {
                            log.error("Unsupported timestamp type");
                            return;
                        }

                        if (EventType.DEPOSIT.getValue().equals(eventType)) {
                            account.apply(new FundsDepositedEvent(eventId, accountId, amount, timestamp));
                        } else if (EventType.WITHDRAWAL.getValue().equals(eventType)) {
                            account.apply(new FundsWithdrawnEvent(eventId, accountId, amount, timestamp));
                        }

                    } catch (Exception e) {
                        log.error("Error: {}", e.getMessage(), e);
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to rebuild account aggregate for " + accountId, e);
        }

        log.info("Account aggregate rebuilt for id: {}", accountId);
        return account;
    }

    private Consumer<String, Map> createConsumerForRebuild() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REBUILD_GROUP_ID_PREFIX + UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, java.util.Map.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "java.util,java.lang,java.math");

        return new KafkaConsumer<>(props);
    }
} 