package com.aegis.wallet.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Entity
@Table(name = "wallet_orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WalletOrderRecord {
    @Id
    private String orderId;
    private String userId;
    private String ticker;
    private String side;          // BUY or SELL
    private BigDecimal limitPrice;
    private int originalQty;
    private int remainingQty;
    private String status;        // OPEN, FILLED, CANCELLED
}
