package com.aegis.wallet.service;

import com.aegis.events.*;
import com.aegis.wallet.entity.*;
import com.aegis.wallet.repo.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class WalletLedgerService {

    private final TraderAccountRepository accountRepo;
    private final ShareHoldingRepository holdingRepo;
    private final WalletOrderRepository orderRepo;
    private final SettledTradeRepository settledRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @PostConstruct
    @Transactional
    public void seedDefaultAccounts() {
        List<String> demoTraders = List.of("trader-1", "trader-2", "spoofer-bot", "wash-bot-a", "wash-bot-b", "flood-bot");
        List<String> tickers = List.of("AAPL", "MSFT", "GOOGL", "NVDA", "TSLA", "AMD");

        for (String user : demoTraders) {
            if (!accountRepo.existsById(user)) {
                accountRepo.save(new TraderAccount(user, new BigDecimal("500000.00"), BigDecimal.ZERO));
                for (String ticker : tickers) {
                    holdingRepo.save(new ShareHolding(null, user, ticker, 5000, 0));
                }
            }
        }
    }

    @KafkaListener(topics = Topics.ORDER_REQUESTS, groupId = "wallet-ssot-group")
    @Transactional
    public void onOrderRequested(MessageEnvelope<?> rawEnvelope) {
        String eventId = rawEnvelope.getHeader().getEventId();
        if (settledRepo.existsById(eventId)) {
            return; // Idempotent deduplication by eventId
        }
        settledRepo.save(new SettledTradeRecord(eventId, Instant.now()));

        OrderPayload order = objectMapper.convertValue(rawEnvelope.getBody(), OrderPayload.class);

        if ("CANCEL".equalsIgnoreCase(order.orderType())) {
            handleCancelValidation(order);
            return;
        }

        TraderAccount account = accountRepo.findById(order.userId()).orElse(null);
        if (account == null) {
            publishRejected(order, "Account does not exist: " + order.userId());
            return;
        }

        if ("BUY".equalsIgnoreCase(order.side())) {
            BigDecimal requiredCash = order.price().multiply(BigDecimal.valueOf(order.quantity()));
            if (account.getAvailableCash().compareTo(requiredCash) < 0) {
                publishRejected(order, "Insufficient available cash. Needed: " + requiredCash);
                return;
            }
            account.setAvailableCash(account.getAvailableCash().subtract(requiredCash));
            account.setReservedCash(account.getReservedCash().add(requiredCash));
            accountRepo.save(account);
        } else {
            ShareHolding holding = holdingRepo.findByUserIdAndTicker(order.userId(), order.ticker()).orElse(null);
            if (holding == null || holding.getAvailableShares() < order.quantity()) {
                publishRejected(order, "Insufficient available shares for " + order.ticker());
                return;
            }
            holding.setAvailableShares(holding.getAvailableShares() - order.quantity());
            holding.setReservedShares(holding.getReservedShares() + order.quantity());
            holdingRepo.save(holding);
        }

        orderRepo.save(new WalletOrderRecord(
                order.orderId(),
                order.userId(),
                order.ticker(),
                order.side(),
                order.price(),
                order.quantity(),
                order.quantity(),
                "OPEN"
        ));

        // Publish order-accepted keyed by ticker so all orders of one stock land on the same partition
        MessageEnvelope<OrderPayload> acceptedEnv = MessageEnvelope.wrap(
                "COMMAND",
                "OrderAccepted",
                Topics.ORDER_ACCEPTED,
                order.ticker(),
                "node-2-wallet",
                order.orderId(),
                order
        );
        kafkaTemplate.send(Topics.ORDER_ACCEPTED, order.ticker(), acceptedEnv);
    }

    private void handleCancelValidation(OrderPayload cancelReq) {
        WalletOrderRecord existing = orderRepo.findById(cancelReq.orderId()).orElse(null);
        if (existing == null || !existing.getUserId().equals(cancelReq.userId()) || !"OPEN".equals(existing.getStatus())) {
            publishRejected(cancelReq, "Cannot cancel order: not found, not owned, or already closed");
            return;
        }

        MessageEnvelope<OrderPayload> acceptedCancel = MessageEnvelope.wrap(
                "COMMAND",
                "OrderAccepted",
                Topics.ORDER_ACCEPTED,
                existing.getTicker(),
                "node-2-wallet",
                existing.getOrderId(),
                new OrderPayload(
                        existing.getOrderId(),
                        existing.getUserId(),
                        existing.getTicker(),
                        existing.getSide(),
                        "CANCEL",
                        existing.getLimitPrice(),
                        existing.getRemainingQty(),
                        null
                )
        );
        kafkaTemplate.send(Topics.ORDER_ACCEPTED, existing.getTicker(), acceptedCancel);
    }

    @KafkaListener(topics = Topics.EXECUTION_EVENTS, groupId = "wallet-ssot-group")
    @Transactional
    public void onExecutionEvent(MessageEnvelope<?> rawEnvelope) {
        ExecutionPayload exec = objectMapper.convertValue(rawEnvelope.getBody(), ExecutionPayload.class);

        if (settledRepo.existsById(exec.tradeId())) {
            log.info("Duplicate execution event ignored idempotently: {}", exec.tradeId());
            return;
        }
        settledRepo.save(new SettledTradeRecord(exec.tradeId(), Instant.now()));

        if ("TradeExecuted".equals(exec.executionType())) {
            settleTrade(exec);
        } else if ("OrderCancelled".equals(exec.executionType())) {
            releaseReservationOnCancel(exec);
        }
    }

    private void settleTrade(ExecutionPayload exec) {
        WalletOrderRecord buyOrder = orderRepo.findById(exec.buyOrderId()).orElse(null);
        WalletOrderRecord sellOrder = orderRepo.findById(exec.sellOrderId()).orElse(null);

        BigDecimal tradeValue = exec.price().multiply(BigDecimal.valueOf(exec.quantity()));
        BigDecimal buyerReservedUnit = buyOrder != null ? buyOrder.getLimitPrice() : exec.price();
        BigDecimal buyerReservedAmount = buyerReservedUnit.multiply(BigDecimal.valueOf(exec.quantity()));
        BigDecimal priceImprovementRefund = buyerReservedAmount.subtract(tradeValue);

        // 1. Update Buyer Cash & Shares
        accountRepo.findById(exec.buyerUserId()).ifPresent(buyer -> {
            buyer.setReservedCash(buyer.getReservedCash().subtract(buyerReservedAmount));
            buyer.setAvailableCash(buyer.getAvailableCash().add(priceImprovementRefund));
            accountRepo.save(buyer);
        });
        ShareHolding buyerHolding = holdingRepo.findByUserIdAndTicker(exec.buyerUserId(), exec.ticker())
                .orElseGet(() -> new ShareHolding(null, exec.buyerUserId(), exec.ticker(), 0, 0));
        buyerHolding.setAvailableShares(buyerHolding.getAvailableShares() + exec.quantity());
        holdingRepo.save(buyerHolding);

        // 2. Update Seller Cash & Shares
        accountRepo.findById(exec.sellerUserId()).ifPresent(seller -> {
            seller.setAvailableCash(seller.getAvailableCash().add(tradeValue));
            accountRepo.save(seller);
        });
        holdingRepo.findByUserIdAndTicker(exec.sellerUserId(), exec.ticker()).ifPresent(sellerHolding -> {
            sellerHolding.setReservedShares(sellerHolding.getReservedShares() - exec.quantity());
            holdingRepo.save(sellerHolding);
        });

        // 3. Update Order Remaining Quantities
        if (buyOrder != null) {
            buyOrder.setRemainingQty(buyOrder.getRemainingQty() - exec.quantity());
            if (buyOrder.getRemainingQty() <= 0) buyOrder.setStatus("FILLED");
            orderRepo.save(buyOrder);
        }
        if (sellOrder != null) {
            sellOrder.setRemainingQty(sellOrder.getRemainingQty() - exec.quantity());
            if (sellOrder.getRemainingQty() <= 0) sellOrder.setStatus("FILLED");
            orderRepo.save(sellOrder);
        }
    }

    private void releaseReservationOnCancel(ExecutionPayload exec) {
        orderRepo.findById(exec.cancelledOrderId()).ifPresent(order -> {
            if (!"OPEN".equals(order.getStatus())) return;
            int qtyToRelease = exec.quantity();
            order.setStatus("CANCELLED");
            order.setRemainingQty(0);
            orderRepo.save(order);

            if ("BUY".equalsIgnoreCase(order.getSide())) {
                BigDecimal unlockCash = order.getLimitPrice().multiply(BigDecimal.valueOf(qtyToRelease));
                accountRepo.findById(order.getUserId()).ifPresent(acc -> {
                    acc.setReservedCash(acc.getReservedCash().subtract(unlockCash));
                    acc.setAvailableCash(acc.getAvailableCash().add(unlockCash));
                    accountRepo.save(acc);
                });
            } else {
                holdingRepo.findByUserIdAndTicker(order.getUserId(), order.getTicker()).ifPresent(hold -> {
                    hold.setReservedShares(hold.getReservedShares() - qtyToRelease);
                    hold.setAvailableShares(hold.getAvailableShares() + qtyToRelease);
                    holdingRepo.save(hold);
                });
            }
        });
    }

    private void publishRejected(OrderPayload order, String reason) {
        OrderPayload rejected = new OrderPayload(
                order.orderId(), order.userId(), order.ticker(), order.side(),
                order.orderType(), order.price(), order.quantity(), reason
        );
        MessageEnvelope<OrderPayload> env = MessageEnvelope.wrap(
                "CONTROL",
                "OrderRejected",
                Topics.ORDER_REJECTED,
                order.userId(),
                "node-2-wallet",
                order.orderId(),
                rejected
        );
        kafkaTemplate.send(Topics.ORDER_REJECTED, order.userId(), env);
    }
}
