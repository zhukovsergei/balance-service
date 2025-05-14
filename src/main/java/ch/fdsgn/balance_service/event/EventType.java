package ch.fdsgn.balance_service.event;

import lombok.Getter;

@Getter
public enum EventType {
    DEPOSIT("DEPOSIT"),
    WITHDRAWAL("WITHDRAWAL");

    private final String value;

    EventType(String value) {
        this.value = value;
    }

    public static EventType fromValue(String value) {
        for (EventType type : EventType.values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown event type: " + value);
    }
} 