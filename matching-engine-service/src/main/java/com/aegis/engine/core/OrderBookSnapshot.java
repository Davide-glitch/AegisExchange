package com.aegis.engine.core;

import java.math.BigDecimal;

public record OrderBookSnapshot(
        String ticker,
        BigDecimal lastPrice,
        BigDecimal bestBid,
        BigDecimal bestAsk,
        int totalBidQuantity,
        int totalAskQuantity
) {}
