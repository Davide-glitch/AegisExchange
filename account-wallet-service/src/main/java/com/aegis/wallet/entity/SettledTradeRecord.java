package com.aegis.wallet.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Entity
@Table(name = "settled_trades")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SettledTradeRecord {
    @Id
    private String deduplicationKey; // tradeId or eventId
    private Instant processedAt;
}
