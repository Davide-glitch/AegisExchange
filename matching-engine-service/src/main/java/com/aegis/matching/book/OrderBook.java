package com.aegis.matching.book;

import com.aegis.events.ExecutionPayload;
import com.aegis.events.OrderPayload;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class OrderBook {

    @Data
    @AllArgsConstructor
    public static class BookEntry {
        private String orderId;
        private String userId;
        private String ticker;
        private String side;
        private BigDecimal price;
        private int remainingQty;
        private long sequenceNumber;
    }

    private final String ticker;
    private final ReentrantLock lock = new ReentrantLock();

    // Highest BUY price first; ties broken by earliest sequenceNumber
    private final PriorityQueue<BookEntry> bids = new PriorityQueue<>(
            Comparator.comparing(BookEntry::getPrice, Comparator.reverseOrder())
                    .thenComparingLong(BookEntry::getSequenceNumber)
    );

    // Lowest SELL price first; ties broken by earliest sequenceNumber
    private final PriorityQueue<BookEntry> asks = new PriorityQueue<>(
            Comparator.comparing(BookEntry::getPrice)
                    .thenComparingLong(BookEntry::getSequenceNumber)
    );

    private final Map<String, BookEntry> activeOrdersById = new HashMap<>();

    public OrderBook(String ticker) {
        this.ticker = ticker;
    }

    public List<ExecutionPayload> processOrder(OrderPayload incoming, int partition, long offset) {
        lock.lock();
        try {
            if ("CANCEL".equalsIgnoreCase(incoming.orderType())) {
                return cancelOrderInternal(incoming.orderId(), partition, offset);
            }
            return matchNewOrderInternal(incoming, partition, offset);
        } finally {
            lock.unlock();
        }
    }

    private List<ExecutionPayload> cancelOrderInternal(String orderId, int partition, long offset) {
        BookEntry removed = activeOrdersById.remove(orderId);
        if (removed == null) {
            return Collections.emptyList();
        }
        if ("BUY".equalsIgnoreCase(removed.getSide())) {
            bids.remove(removed);
        } else {
            asks.remove(removed);
        }

        String tradeId = partition + "-" + offset + "-cancel";
        return List.of(new ExecutionPayload(
                "OrderCancelled",
                tradeId,
                ticker,
                null,
                null,
                null,
                null,
                removed.getOrderId(),
                removed.getUserId(),
                removed.getPrice(),
                removed.getRemainingQty(),
                Instant.now()
        ));
    }

    private List<ExecutionPayload> matchNewOrderInternal(OrderPayload incoming, int partition, long offset) {
        List<ExecutionPayload> executions = new ArrayList<>();
        int remaining = incoming.quantity();
        int matchSeq = 0;

        if ("BUY".equalsIgnoreCase(incoming.side())) {
            while (remaining > 0 && !asks.isEmpty() && asks.peek().getPrice().compareTo(incoming.price()) <= 0) {
                BookEntry bestAsk = asks.peek();
                int fillQty = Math.min(remaining, bestAsk.getRemainingQty());
                BigDecimal fillPrice = bestAsk.getPrice();
                matchSeq++;

                String tradeId = partition + "-" + offset + "-" + matchSeq;
                executions.add(new ExecutionPayload(
                        "TradeExecuted",
                        tradeId,
                        ticker,
                        incoming.orderId(),
                        bestAsk.getOrderId(),
                        incoming.userId(),
                        bestAsk.getUserId(),
                        null,
                        null,
                        fillPrice,
                        fillQty,
                        Instant.now()
                ));

                remaining -= fillQty;
                bestAsk.setRemainingQty(bestAsk.getRemainingQty() - fillQty);
                if (bestAsk.getRemainingQty() <= 0) {
                    asks.poll();
                    activeOrdersById.remove(bestAsk.getOrderId());
                }
            }
            if (remaining > 0) {
                BookEntry restingBid = new BookEntry(
                        incoming.orderId(), incoming.userId(), ticker, "BUY", incoming.price(), remaining, offset
                );
                bids.add(restingBid);
                activeOrdersById.put(restingBid.getOrderId(), restingBid);
            }
        } else {
            while (remaining > 0 && !bids.isEmpty() && bids.peek().getPrice().compareTo(incoming.price()) >= 0) {
                BookEntry bestBid = bids.peek();
                int fillQty = Math.min(remaining, bestBid.getRemainingQty());
                BigDecimal fillPrice = bestBid.getPrice();
                matchSeq++;

                String tradeId = partition + "-" + offset + "-" + matchSeq;
                executions.add(new ExecutionPayload(
                        "TradeExecuted",
                        tradeId,
                        ticker,
                        bestBid.getOrderId(),
                        incoming.orderId(),
                        bestBid.getUserId(),
                        incoming.userId(),
                        null,
                        null,
                        fillPrice,
                        fillQty,
                        Instant.now()
                ));

                remaining -= fillQty;
                bestBid.setRemainingQty(bestBid.getRemainingQty() - fillQty);
                if (bestBid.getRemainingQty() <= 0) {
                    bids.poll();
                    activeOrdersById.remove(bestBid.getOrderId());
                }
            }
            if (remaining > 0) {
                BookEntry restingAsk = new BookEntry(
                        incoming.orderId(), incoming.userId(), ticker, "SELL", incoming.price(), remaining, offset
                );
                asks.add(restingAsk);
                activeOrdersById.put(restingAsk.getOrderId(), restingAsk);
            }
        }
        return executions;
    }

    public void clearForRebuild() {
        lock.lock();
        try {
            bids.clear();
            asks.clear();
            activeOrdersById.clear();
        } finally {
            lock.unlock();
        }
    }
}
