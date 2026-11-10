package com.aegis.engine.pipeline;

import com.aegis.engine.core.OrderBook;
import com.aegis.events.ExecutionPayload;
import com.aegis.events.OrderPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

/**
 * Producer-Consumer pipeline for parallel ticker partition matching.
 * 
 * Verifications & Critical Fixes Applied:
 * 1. Type signature uses BlockingQueue<OrderPayload> (resolved generic syntax corruption).
 * 2. Inbound dispatch uses queue.put(order) to ensure true producer-consumer backpressure
 *    per Professor Cosma's slides (74 & 283) and Architecture v3.2 specification.
 */
public class MatchingEnginePipeline {
    private static final Logger log = LoggerFactory.getLogger(MatchingEnginePipeline.class);
    private static final int QUEUE_CAPACITY = 10_000;

    private final ConcurrentMap<String, BlockingQueue<OrderPayload>> tickerQueues = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, OrderBook> books = new ConcurrentHashMap<>();
    private final Consumer<List<ExecutionPayload>> executionPublisher;
    private volatile boolean running = true;

    public MatchingEnginePipeline(Consumer<List<ExecutionPayload>> executionPublisher) {
        this.executionPublisher = executionPublisher;
    }

    /**
     * Enqueues an inbound validated order for matching.
     * Uses put() to exert backpressure if the bounded queue is full.
     */
    public void enqueue(OrderPayload order, int partition, long offset) throws InterruptedException {
        String ticker = order.ticker();
        BlockingQueue<OrderPayload> queue = tickerQueues.computeIfAbsent(ticker, t -> {
            BlockingQueue<OrderPayload> q = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
            books.putIfAbsent(t, new OrderBook(t, BigDecimal.valueOf(100.00)));
            startWorkerThread(t, q);
            return q;
        });

        // Enforce true backpressure: producer blocks when bounded buffer is full
        queue.put(order);
    }

    private void startWorkerThread(String ticker, BlockingQueue<OrderPayload> queue) {
        Thread worker = new Thread(() -> {
            log.info("Matching Engine worker thread started for ticker: {}", ticker);
            OrderBook book = books.get(ticker);
            long virtualOffset = 0;

            while (running) {
                try {
                    OrderPayload order = queue.take(); // Blocks until an order is available
                    List<ExecutionPayload> executions = book.process(order, 0, virtualOffset++);
                    if (!executions.isEmpty() && executionPublisher != null) {
                        executionPublisher.accept(executions);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Worker thread for ticker {} interrupted", ticker);
                    break;
                } catch (Exception e) {
                    log.error("Error processing order for ticker {}: {}", ticker, e.getMessage(), e);
                }
            }
        }, "MatchingWorker-" + ticker);

        worker.setDaemon(true);
        worker.start();
    }

    public OrderBook getOrderBook(String ticker) {
        return books.get(ticker);
    }

    public void shutdown() {
        this.running = false;
    }
}
