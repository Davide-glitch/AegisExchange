package com.aegis.wallet.repo;

import com.aegis.wallet.entity.SettledTradeRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SettledTradeRepository extends JpaRepository<SettledTradeRecord, String> {}
