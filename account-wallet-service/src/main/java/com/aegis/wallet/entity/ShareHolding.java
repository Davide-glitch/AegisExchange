package com.aegis.wallet.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "share_holdings", uniqueConstraints = @UniqueConstraint(columnNames = {"userId", "ticker"}))
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ShareHolding {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String userId;
    private String ticker;
    private int availableShares;
    private int reservedShares;
}
