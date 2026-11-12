package com.aegis.wallet.repo;

import com.aegis.wallet.entity.ShareHolding;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import java.util.Optional;

public interface ShareHoldingRepository extends JpaRepository<ShareHolding, Long> {
    Optional<ShareHolding> findByUserIdAndTicker(String userId, String ticker);
    List<ShareHolding> findByUserId(String userId);
}
