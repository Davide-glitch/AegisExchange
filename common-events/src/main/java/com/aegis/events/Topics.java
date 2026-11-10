package com.aegis.events;

public final class Topics {
    private Topics() {}

    public static final String ORDER_REQUESTS     = "order-requests";     // key = userId
    public static final String ORDER_ACCEPTED     = "order-accepted";     // key = ticker
    public static final String ORDER_REJECTED     = "order-rejected";     // key = userId
    public static final String EXECUTION_EVENTS   = "execution-events";   // key = ticker (TradeExecuted & OrderCancelled)
    public static final String ENFORCEMENT_EVENTS = "enforcement-events"; // key = targetId (accountId or ticker)
}
