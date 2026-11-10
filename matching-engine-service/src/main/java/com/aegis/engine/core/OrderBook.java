package com.aegis.engine.core;

import com.aegis.events.ExecutionPayload;
import com.aegis.events.OrderPayload;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Concurrent OrderBook matching engine adapted from Stage 1.
 * 
 * Key Characteristics:
 * 1. Fine-grained ReentrantLock per ticker book to isolate symbol state.
 * 2. In-memory PriorityQueue heaps for bids (max-heap) and asks (min-heap) providing price-time priority.
 * 3. Returns deterministic ExecutionPayload events with tradeId in format: {partition}-{offset}-{seq}.
 */
public class OrderBook {
    private final String ticker;
    private final ReentrantLock lock = new ReentrantLock();

    // Bids: highest price first, then oldest sequence number first
    private final PriorityQueue<InternalOrder> bids = new PriorityQueue<>(
            Comparator.comparing(InternalOrder::getPrice).reversed()
                    .thenComparing(InternalOrder::getSequenceNumber));

    // Asks: lowest price first, then oldest sequence number first
    private final PriorityQueue<InternalOrder> asks = new PriorityQueue<>(
            Comparator.comparing(InternalOrder::getPrice)
                    .thenComparing(InternalOrder::getSequenceNumber));

    private final AtomicLong internalSequence = new AtomicLong(1);
    private volatile BigDecimal lastPrice = BigDecimal.ZERO;

    public OrderBook(String ticker, BigDecimal openingPrice) {
        this.ticker = ticker;
        this.lastPrice = openingPrice;
    }

    /**
     * Matches a new or cancel order against the book.
     * Generates execution payloads (TradeExecuted or OrderCancelled) with deterministic tradeIds.
     */
    public List<ExecutionPayload> process(OrderPayload incoming, int partition, long offset) {
        List<ExecutionPayload> executions = new ArrayList<>();
        lock.lock();
        try {
            if ("CANCEL".equalsIgnoreCase(incoming.orderType())) {
                handleCancel(incoming, partition, offset, executions);
            } else {
                handleMatch(incoming, partition, offset, executions);
            }
        } finally {
            lock.unlock();
        }
        return executions;
    }

    private void handleMatch(OrderPayload payload, int partition, long offset, List<ExecutionPayload> executions) {
        InternalOrder incoming = new InternalOrder(
                payload.orderId(),
                payload.userId(),
                payload.ticker(),
                payload.side(),
                payload.price(),
                payload.quantity(),
                internalSequence.getAndIncrement()
        );

        boolean isBuy = "BUY".equalsIgnoreCase(incoming.getSide());
        PriorityQueue<InternalOrder> oppositeSide = isBuy ? asks : bids;
        PriorityQueue<InternalOrder> ownSide = isBuy ? bids : asks;
        int matchSeq = 1;

        while (!incoming.isFilled() && !oppositeSide.isEmpty() && incoming.canTradeWith(oppositeSide.peek())) {
            InternalOrder resting = oppositeSide.peek();
            int fillQuantity = Math.min(incoming.getRemainingQuantity(), resting.getRemainingQuantity());

            incoming.fill(fillQuantity);
            resting.fill(fillQuantity);

            String tradeId = String.format("%d-%d-%d", partition, offset, matchSeq++);
            BigDecimal matchPrice = resting.getPrice(); // Maker price
            this.lastPrice = matchPrice;

            String buyerId = isBuy ? incoming.getUserId() : resting.getUserId();
            String sellerId = isBuy ? resting.getUserId() : incoming.getUserId();
            String buyOrderId = isBuy ? incoming.getOrderId() : resting.getOrderId();
            String sellOrderId = isBuy ? resting.getOrderId() : incoming.getOrderId();

            executions.add(new ExecutionPayload(
                    "TradeExecuted",
                    tradeId,
                    this.ticker,
                    buyOrderId,
                    sellOrderId,
                    buyerId,
                    sellerId,
                    null,
                    null,
                    matchPrice,
                    fillQuantity,
                    Instant.now()
            ));

            if (resting.isFilled()) {
                oppositeSide.poll();
            }
        }

        if (!incoming.isFilled()) {
            ownSide.add(incoming);
        }
    }

    private void handleCancel(OrderPayload payload, int partition, long offset, List<ExecutionPayload> executions) {
        boolean isBuy = "BUY".equalsIgnoreCase(payload.side());
        PriorityQueue<InternalOrder> targetQueue = isBuy ? bids : asks;

        InternalOrder target = null;
        for (InternalOrder order : targetQueue) {
            if (order.getOrderId().equals(payload.orderId())) {
                target = order;
                break;
            }
        }

        if (target != null && targetQueue.remove(target)) {
            String cancelId = String.format("%d-%d-cancel", partition, offset);
            executions.add(new ExecutionPayload(
                    "OrderCancelled",
                    cancelId,
                    this.ticker,
                    null,
                    null,
                    null,
                    null,
                    target.getOrderId(),
                    target.getUserId(),
                    target.getPrice(),
                    target.getRemainingQuantity(),
                    Instant.now()
            ));
        }
    }

    public OrderBookSnapshot getSnapshot() {
        lock.lock();
        try {
            BigDecimal bestBid = bids.isEmpty() ? null : bids.peek().getPrice();
            BigDecimal bestAsk = asks.isEmpty() ? null : asks.peek().getPrice();
            int totalBidQty = bids.stream().mapToInt(InternalOrder::getRemainingQuantity).sum();
            int totalAskQty = asks.stream().mapToInt(InternalOrder::getRemainingQuantity).sum();
            return new OrderBookSnapshot(ticker, lastPrice, bestBid, bestAsk, totalBidQty, totalAskQty);
        } finally {
            lock.unlock();
        }
    }

    public BigDecimal getLastPrice() {
        return lastPrice;
    }

    public String getTicker() {
        return ticker;
    }
}
