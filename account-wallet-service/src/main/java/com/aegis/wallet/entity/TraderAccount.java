package com.aegis.wallet.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "trader_accounts")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TraderAccount {
    @Id
    private String userId;
    private BigDecimal availableCash;
    private BigDecimal reservedCash;
}
