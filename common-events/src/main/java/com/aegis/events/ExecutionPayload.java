package com.aegis.events;

import java.math.BigDecimal;
import java.time.Instant;

public record ExecutionPayload(
        String executionType, // "TradeExecuted" or "OrderCancelled"
        String tradeId,       // Deterministic: partition-offset-seq (for idempotent deduplication in Wallet)
        String ticker,
        String buyOrderId,
        String sellOrderId,
        String buyerUserId,
        String sellerUserId,
        String cancelledOrderId,
        String cancelledUserId,
        BigDecimal price,
        int quantity,
        Instant executedAt
) {}
