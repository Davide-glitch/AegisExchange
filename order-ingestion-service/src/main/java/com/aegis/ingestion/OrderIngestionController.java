package com.aegis.ingestion;

import com.aegis.events.MessageEnvelope;
import com.aegis.events.OrderPayload;
import com.aegis.events.Topics;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderIngestionController {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EnforcementCacheService enforcementCache;

    @Value("${node.id:node-1a}")
    private String nodeId;

    @PostMapping
    public ResponseEntity<Map<String, Object>> submitNewOrder(@RequestBody OrderPayload request) {
        if (request.userId() == null || request.ticker() == null || request.side() == null
                || request.price() == null || request.price().compareTo(BigDecimal.ZERO) <= 0
                || request.quantity() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("error", "Invalid order payload syntax"));
        }

        if (enforcementCache.isAccountFrozen(request.userId())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("error", "Account is frozen by SecOps Enforcement: " + request.userId()));
        }

        if (enforcementCache.isTickerHalted(request.ticker())) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Map.of("error", "Trading halted for ticker: " + request.ticker()));
        }

        if (!enforcementCache.checkRateLimitAllowed(request.userId())) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .body(Map.of("error", "Rate limit exceeded for trader: " + request.userId()));
        }

        String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);
        OrderPayload normalized = new OrderPayload(
                orderId,
                request.userId(),
                request.ticker().toUpperCase(),
                request.side().toUpperCase(),
                "NEW",
                request.price(),
                request.quantity(),
                null
        );

        MessageEnvelope<OrderPayload> envelope = MessageEnvelope.wrap(
                "COMMAND",
                "OrderRequested",
                Topics.ORDER_REQUESTS,
                normalized.userId(), // Key = userId (serializes requests per user)
                nodeId,
                orderId,
                normalized
        );

        kafkaTemplate.send(Topics.ORDER_REQUESTS, normalized.userId(), envelope);

        return ResponseEntity.accepted().body(Map.of(
                "status", "ACCEPTED_FOR_VALIDATION",
                "orderId", orderId,
                "handledByNode", nodeId
        ));
    }

    @DeleteMapping("/{orderId}")
    public ResponseEntity<Map<String, Object>> cancelOrder(
            @PathVariable String orderId,
            @RequestParam String userId,
            @RequestParam String ticker) {

        OrderPayload cancelPayload = new OrderPayload(
                orderId,
                userId,
                ticker.toUpperCase(),
                "BUY",
                "CANCEL",
                BigDecimal.ZERO,
                0,
                null
        );

        MessageEnvelope<OrderPayload> envelope = MessageEnvelope.wrap(
                "COMMAND",
                "OrderRequested",
                Topics.ORDER_REQUESTS,
                userId,
                nodeId,
                orderId,
                cancelPayload
        );

        kafkaTemplate.send(Topics.ORDER_REQUESTS, userId, envelope);

        return ResponseEntity.accepted().body(Map.of(
                "status", "CANCEL_REQUESTED",
                "orderId", orderId,
                "handledByNode", nodeId
        ));
    }
}
