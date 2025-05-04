package ch.fdsgn.balance_service.readmodel.repository;

import ch.fdsgn.balance_service.readmodel.entity.AccountBalance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface AccountBalanceRepository extends JpaRepository<AccountBalance, String> {

}