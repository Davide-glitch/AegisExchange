package com.aegis.wallet.repo;

import com.aegis.wallet.entity.WalletOrderRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface WalletOrderRepository extends JpaRepository<WalletOrderRecord, String> {}
