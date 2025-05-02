package ch.fdsgn.balance_service.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private static final String ACCOUNT_EVENTS_TOPIC = "account-events";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    public EventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(String key, Object event) {
        log.info("Publishing event to topic {}: key={}, event={}", ACCOUNT_EVENTS_TOPIC, key, event);
        try {
            kafkaTemplate.send(ACCOUNT_EVENTS_TOPIC, key, event);
        } catch (Exception e) {
            log.error("Failed to publish event key={}, event={}", key, event, e);
        }
    }
} 