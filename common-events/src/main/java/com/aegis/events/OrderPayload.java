package com.aegis.events;

import java.math.BigDecimal;

public record OrderPayload(
        String orderId,
        String userId,
        String ticker,
        String side,          // "BUY" or "SELL"
        String orderType,     // "NEW" or "CANCEL"
        BigDecimal price,
        int quantity,
        String rejectReason   // null unless published to order-rejected
) {}
