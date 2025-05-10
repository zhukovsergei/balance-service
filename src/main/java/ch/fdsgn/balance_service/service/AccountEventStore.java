package ch.fdsgn.balance_service.service;

import ch.fdsgn.balance_service.domain.aggregate.Account;
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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Service
public class AccountEventStore {
    private static final Logger log = LoggerFactory.getLogger(AccountEventStore.class);
    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";
    private static final String REBUILD_GROUP_ID = "account-rebuild-group";

    private final String bootstrapServers;

    public AccountEventStore(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Account rebuildAccount(String accountId) {
        Account account = new Account(accountId);
        try (Consumer<String, Object> consumer = createConsumer()) {
            consumer.subscribe(Collections.singletonList(ACCOUNT_EVENTS_TOPIC));
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    break;
                }
                records.forEach(record -> {
                    Object event = record.value();
                    if (event instanceof FundsDepositedEvent depositedEvent && depositedEvent.accountId().equals(accountId)) {
                        account.apply(depositedEvent);
                    } else if (event instanceof FundsWithdrawnEvent withdrawnEvent && withdrawnEvent.accountId().equals(accountId)) {
                        account.apply(withdrawnEvent);
                    }
                });
            }
        } catch (Exception e) {
            log.error("Error rebuilding: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to rebuild account aggregate", e);
        }
        log.info("Account aggregate rebuilt for id {}", accountId);
        return account;
    }

    private Consumer<String, Object> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REBUILD_GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "ch.fdsgn.balance_service.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        return new KafkaConsumer<>(props);
    }
} 