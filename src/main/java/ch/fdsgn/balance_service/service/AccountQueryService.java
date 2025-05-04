package ch.fdsgn.balance_service.service;

import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import ch.fdsgn.balance_service.readmodel.repository.AccountBalanceRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Optional;

@Service
public class AccountQueryService {

    private static final Logger log = LoggerFactory.getLogger(AccountQueryService.class);

    private final AccountBalanceRepository accountBalanceRepository;

    @Autowired
    public AccountQueryService(AccountBalanceRepository accountBalanceRepository) {
        this.accountBalanceRepository = accountBalanceRepository;
    }

    @Transactional(readOnly = true)
    public Optional<BigDecimal> getAccountBalanceScalar(String accountId) {
        log.debug("Querying balance for accountId: {}", accountId);
        Optional<AccountBalance> balanceOptional = accountBalanceRepository.findById(accountId);

        return balanceOptional.map(AccountBalance::getCurrentBalance);
    }

     @Transactional(readOnly = true)
     public Optional<AccountBalance> getAccountBalance(String accountId) {
         return accountBalanceRepository.findById(accountId);
     }
} 