package com.aegis.events;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageEnvelope<T> {
    private Header header;
    private Properties properties;
    private T body;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Header {
        private String eventId;
        private String messageCategory; // COMMAND, DATA_TRANSFER, CONTROL
        private String eventType;       // OrderRequested, OrderAccepted, OrderRejected, TradeExecuted, OrderCancelled, EnforcementActivated
        private String topicChannel;
        private String partitionKey;    // userId, ticker, or targetId
        private long sequenceOffset;
        @Builder.Default
        private Instant timestamp = Instant.now();
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Properties {
        private String sourceNode;
        private String correlationId;
        @Builder.Default
        private String schemaVersion = "1.0";
    }

    public static <T> MessageEnvelope<T> wrap(
            String category,
            String eventType,
            String topic,
            String partitionKey,
            String sourceNode,
            String correlationId,
            T payload) {
        return MessageEnvelope.<T>builder()
                .header(Header.builder()
                        .eventId("evt-" + UUID.randomUUID())
                        .messageCategory(category)
                        .eventType(eventType)
                        .topicChannel(topic)
                        .partitionKey(partitionKey)
                        .timestamp(Instant.now())
                        .build())
                .properties(Properties.builder()
                        .sourceNode(sourceNode)
                        .correlationId(correlationId)
                        .schemaVersion("1.0")
                        .build())
                .body(payload)
                .build();
    }
}
