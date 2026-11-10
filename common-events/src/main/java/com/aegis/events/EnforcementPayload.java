package com.aegis.events;

import java.time.Instant;

public record EnforcementPayload(
        String proposalId,
        String targetId,      // userId (for FREEZE/THROTTLE) or ticker (for HALT_TICKER)
        String actionType,    // "FREEZE_ACCOUNT", "THROTTLE_TRADER", "HALT_TICKER"
        String reason,
        String status,        // "PENDING" or "APPROVED"
        Instant timestamp
) {}
