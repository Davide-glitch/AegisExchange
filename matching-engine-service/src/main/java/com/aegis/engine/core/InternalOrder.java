package com.aegis.engine.core;

import java.math.BigDecimal;

public class InternalOrder {
    private final String orderId;
    private final String userId;
    private final String ticker;
    private final String side; // "BUY" or "SELL"
    private final BigDecimal price;
    private final int initialQuantity;
    private int remainingQuantity;
    private final long sequenceNumber;

    public InternalOrder(String orderId, String userId, String ticker, String side, BigDecimal price, int quantity, long sequenceNumber) {
        this.orderId = orderId;
        this.userId = userId;
        this.ticker = ticker;
        this.side = side;
        this.price = price;
        this.initialQuantity = quantity;
        this.remainingQuantity = quantity;
        this.sequenceNumber = sequenceNumber;
    }

    public boolean canTradeWith(InternalOrder resting) {
        if ("BUY".equalsIgnoreCase(this.side)) {
            return this.price.compareTo(resting.getPrice()) >= 0;
        } else {
            return this.price.compareTo(resting.getPrice()) <= 0;
        }
    }

    public void fill(int quantity) {
        this.remainingQuantity -= quantity;
    }

    public boolean isFilled() {
        return this.remainingQuantity <= 0;
    }

    public String getOrderId() { return orderId; }
    public String getUserId() { return userId; }
    public String getTicker() { return ticker; }
    public String getSide() { return side; }
    public BigDecimal getPrice() { return price; }
    public int getInitialQuantity() { return initialQuantity; }
    public int getRemainingQuantity() { return remainingQuantity; }
    public long getSequenceNumber() { return sequenceNumber; }
}
