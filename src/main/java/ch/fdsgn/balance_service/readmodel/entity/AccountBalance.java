package ch.fdsgn.balance_service.readmodel.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

@Entity
@Table(name = "account_balances")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class AccountBalance {

    @Id
    @Column(name = "id", nullable = false, unique = true)
    private String id;

    @Column(name = "current_balance", nullable = false, precision = 19, scale = 4)
    private BigDecimal currentBalance = BigDecimal.ZERO;

    @Column(name = "last_updated_at", nullable = false)
    private Instant lastUpdatedAt;

    @ElementCollection(fetch = FetchType.EAGER)
    @CollectionTable(name = "processed_event_ids", joinColumns = @JoinColumn(name = "account_balance_id"))
    @Column(name = "id", nullable = false)
    private Set<UUID> processedEventIds = new HashSet<>();

    @PrePersist
    @PreUpdate
    public void updateTimestamp() {
        lastUpdatedAt = Instant.now();
    }

    public AccountBalance(String accountId, BigDecimal initialBalance, UUID firstEventId) {
        this.id = accountId;
        this.currentBalance = initialBalance;
        if (firstEventId != null) {
            this.processedEventIds.add(firstEventId);
        }
    }

    public AccountBalance(String accountId, BigDecimal initialBalance) {
        this(accountId, initialBalance, null);
    }
} 