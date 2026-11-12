package com.aegis.ingestion;

import com.aegis.events.EnforcementPayload;
import com.aegis.events.MessageEnvelope;
import com.aegis.events.Topics;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
@RequiredArgsConstructor
public class EnforcementCacheService {

    private final ObjectMapper objectMapper;
    private final Set<String> frozenAccounts = ConcurrentHashMap.newKeySet();
    private final Set<String> throttledAccounts = ConcurrentHashMap.newKeySet();
    private final Set<String> haltedTickers = ConcurrentHashMap.newKeySet();
    private final Map<String, WindowCounter> rateCounters = new ConcurrentHashMap<>();

    @KafkaListener(topics = Topics.ENFORCEMENT_EVENTS)
    public void onEnforcementEvent(MessageEnvelope<?> rawEnvelope) {
        EnforcementPayload payload = objectMapper.convertValue(rawEnvelope.getBody(), EnforcementPayload.class);
        log.info("Edge Ingestion received enforcement action: {} on target {}", payload.actionType(), payload.targetId());

        switch (payload.actionType()) {
            case "FREEZE_ACCOUNT" -> frozenAccounts.add(payload.targetId());
            case "THROTTLE_TRADER" -> throttledAccounts.add(payload.targetId());
            case "HALT_TICKER" -> haltedTickers.add(payload.targetId());
        }
    }

    public boolean isAccountFrozen(String userId) {
        return frozenAccounts.contains(userId);
    }

    public boolean isTickerHalted(String ticker) {
        return haltedTickers.contains(ticker);
    }

    public boolean checkRateLimitAllowed(String userId) {
        int maxOrdersPerSecond = throttledAccounts.contains(userId) ? 2 : 50;
        long currentSecond = System.currentTimeMillis() / 1000;
        WindowCounter counter = rateCounters.computeIfAbsent(userId, k -> new WindowCounter(currentSecond));
        return counter.incrementAndCheck(currentSecond, maxOrdersPerSecond);
    }

    private static class WindowCounter {
        private long windowSecond;
        private final AtomicInteger count = new AtomicInteger(0);

        WindowCounter(long windowSecond) {
            this.windowSecond = windowSecond;
        }

        synchronized boolean incrementAndCheck(long currentSecond, int limit) {
            if (currentSecond != windowSecond) {
                windowSecond = currentSecond;
                count.set(0);
            }
            return count.incrementAndGet() <= limit;
        }
    }
}
