# ProTrade-X Trading Engine - Architectural Enhancement Plan

## Executive Summary

This document outlines a comprehensive architectural upgrade to the ProTrade-X trading engine, focusing on three major enhancement categories:

1. **Advanced Order Types & Matching Logic** - Support for complex order types (OCO, FOK, STP)
2. **Algorithmic Trading & Backtesting Module** - Enterprise-grade strategy testing and real-time API
3. **Latency Monitoring & Resilience** - Microsecond instrumentation and hot-swap failover

---

## Part 1: Advanced Order Types & Matching Logic

### 1.1 Current State Assessment

**Current Implementation:**
- Simple Price-Time Priority matching
- Basic Order and ProductBook data structures
- ReentrantReadWriteLock for thread safety
- Linear order book representation

**Limitations:**
- No support for complex order relationships
- No self-trade prevention
- No conditional order logic

### 1.2 One-Cancels-Other (OCO) Orders

#### Design Specification

**Data Structure Modifications:**

```java
// Enhanced Order class to support order linking
public class Order {
    private String orderId;
    private String linkedOrderId;        // NEW: For OCO relationships
    private OrderLinkType linkType;      // NEW: OCO, OSO, etc.
    private OrderStatus status;
    private long timestamp;
    // ... existing fields
}

// New enum for order linking
public enum OrderLinkType {
    STANDALONE,
    ONE_CANCELS_OTHER,
    ONE_SENDS_OTHER,
    ONE_TRIGGERS_OTHER
}

// Order relationship registry
public class OrderRelationshipRegistry {
    private final ConcurrentHashMap<String, OrderRelationship> relationships;
    
    public void linkOrders(String orderId1, String orderId2, OrderLinkType type) {
        OrderRelationship rel = new OrderRelationship(orderId1, orderId2, type);
        relationships.put(orderId1, rel);
        relationships.put(orderId2, rel);
    }
    
    public OrderRelationship getRelationship(String orderId) {
        return relationships.get(orderId);
    }
}
```

**Matching Engine Modifications:**

When an order is filled, execute the linked order cancellation:

```
1. Order A (BUY) is matched and partially filled
2. Check OrderRelationshipRegistry for linked orders
3. If OCO found with Order B:
   - Immediately cancel Order B with status CANCELLED_OCO
   - Log both events to audit log
   - Notify interested parties via WebSocket
4. Clear the relationship from registry
```

**Implementation Steps:**
1. Create `OrderRelationship` class to store linking metadata
2. Modify `ProductBook.executeOrder()` to check linked orders after execution
3. Implement `OrderRelationshipRegistry` as singleton
4. Add OCO validation logic before order acceptance
5. Extend audit logging to include relationship events

---

### 1.3 Fill or Kill (FOK) Orders

#### Design Specification

**Matching Logic:**

```java
public class FillOrKillValidator {
    
    public boolean validateFOK(Order fofOrder, ProductBookSide bookSide) {
        // Gather all available liquidity at better or same price
        long availableLiquidity = bookSide.getAvailableLiquidity(
            fofOrder.getPrice(), 
            fofOrder.getOrderId()
        );
        
        // If not enough liquidity to fill entire order, reject immediately
        if (availableLiquidity < fofOrder.getQuantity()) {
            return false;
        }
        
        return true;
    }
}
```

**Execution Path:**

```
1. FOK order arrives at ProductBook
2. Call FillOrKillValidator.validateFOK()
3. If validation fails:
   - Reject order immediately
   - Status: REJECTED_FOK
   - Log rejection event
   - Return error to client
4. If validation passes:
   - Execute order matching normally
   - Guarantee full fill
```

**Implementation Steps:**
1. Create `FillOrKillValidator` class
2. Modify `ProductBook.addOrder()` to handle FOK validation
3. Add FOK-specific status codes to `OrderStatus` enum
4. Implement liquidity aggregation logic
5. Add performance benchmark for FOK check (target <1ms)

---

### 1.4 Self-Trade Prevention (STP)

#### Design Specification

**Core Logic:**

```java
public class SelfTradePreventionEngine {
    
    public boolean isSelfTrade(Order incomingOrder, Order bookOrder) {
        // Extract trader IDs (assuming format: "TRADER_ID_SYMBOL_PRICE")
        String incomingTrader = extractTraderId(incomingOrder.getOrderId());
        String bookTrader = extractTraderId(bookOrder.getOrderId());
        
        return incomingTrader.equals(bookTrader);
    }
    
    public STPAction determineSTPAction(Order incomingOrder, 
                                         List<Order> potentialCounterparties) {
        // Check each potential match
        for (Order bookOrder : potentialCounterparties) {
            if (isSelfTrade(incomingOrder, bookOrder)) {
                // Options: CANCEL_INCOMING, CANCEL_RESTING, CANCEL_BOTH
                return STPAction.CANCEL_RESTING;
            }
        }
        return STPAction.ALLOW;
    }
}

public enum STPAction {
    ALLOW,
    CANCEL_INCOMING,
    CANCEL_RESTING,
    CANCEL_BOTH
}
```

**Integration Points:**

```
1. Before order matching in ProductBook:
   - For each price level that could match
   - Check STP condition
   - If STP detected, skip that order
   - Continue to next price level
   
2. Audit logging must capture STP events:
   - STP_VIOLATION event type
   - Both order IDs involved
   - Action taken (which order cancelled)
```

**Implementation Steps:**
1. Create `SelfTradePreventionEngine` class
2. Integrate STP checks into `ProductBook.matchOrder()`
3. Define order ID parsing strategy (extract trader identifier)
4. Add `STP_VIOLATION` to `AuditEventType` enum
5. Log all STP events with both order details
6. Add configuration for STP mode (CANCEL_RESTING default)

---

### 1.5 Enhanced ProductBook Architecture

```java
public class ProductBook {
    private final ProductBookSide buyBook;
    private final ProductBookSide sellBook;
    private final OrderRelationshipRegistry relationshipRegistry;  // NEW
    private final SelfTradePreventionEngine stpEngine;             // NEW
    private final FillOrKillValidator fokValidator;               // NEW
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    
    public void addOrder(Order order) throws InvalidPriceOperation {
        lock.writeLock().lock();
        try {
            // 1. Validate order type
            validateOrderType(order);
            
            // 2. Check FOK requirements
            if (order.isFOK()) {
                if (!fokValidator.validateFOK(order, getOppositeBookSide(order))) {
                    order.setStatus(OrderStatus.REJECTED_FOK);
                    AuditLogger.getInstance().logEvent(new AuditEvent.Builder()
                        .eventType(AuditEventType.ORDER_REJECTED)
                        .product(order.getProduct())
                        .addData("reason", "FOK_INSUFFICIENT_LIQUIDITY")
                        .build());
                    return;
                }
            }
            
            // 3. Execute matching
            executeMatching(order);
            
            // 4. Check OCO relationships
            if (order.getLinkType() == OrderLinkType.ONE_CANCELS_OTHER) {
                relationshipRegistry.linkOrders(
                    order.getOrderId(), 
                    order.getLinkedOrderId(),
                    order.getLinkType()
                );
            }
            
        } finally {
            lock.writeLock().unlock();
        }
    }
    
    private void executeMatching(Order incomingOrder) {
        ProductBookSide oppositeBook = getOppositeBookSide(incomingOrder);
        
        while (incomingOrder.getRemainingQuantity() > 0 && !oppositeBook.isEmpty()) {
            Order resting = oppositeBook.getBestOrder();
            
            // STP Check
            if (stpEngine.isSelfTrade(incomingOrder, resting)) {
                oppositeBook.skipOrder(resting);
                continue;
            }
            
            // Execute trade
            executeTrade(incomingOrder, resting);
            
            // Check OCO after execution
            OrderRelationship rel = relationshipRegistry.getRelationship(resting.getOrderId());
            if (rel != null && rel.getType() == OrderLinkType.ONE_CANCELS_OTHER) {
                cancelLinkedOrder(rel.getLinkedOrderId());
            }
        }
    }
}
```

---

## Part 2: Algorithmic Trading & Backtesting Module

### 2.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│           Backtesting Service (Offline)                  │
├─────────────────────────────────────────────────────────┤
│  - Historical Data Loader                               │
│  - Strategy Simulator                                   │
│  - Metrics Calculator                                   │
│  - Report Generator                                     │
└────────────┬────────────────────────────────────────────┘
             │
             ├─ Reads from PostgreSQL (audit_log table)
             │
┌────────────▼────────────────────────────────────────────┐
│      Real-Time Trading API (Online)                      │
├─────────────────────────────────────────────────────────┤
│  - gRPC Order Submission Service                        │
│  - WebSocket Market Data Stream                         │
│  - REST Admin API                                       │
└─────────────────────────────────────────────────────────┘
             │
             ├─ Connects to Live ProductBook
             │
┌────────────▼────────────────────────────────────────────┐
│      Core Trading Engine (Existing)                      │
├─────────────────────────────────────────────────────────┤
│  - Order Matching                                       │
│  - Market Tracking                                      │
│  - Audit Logging                                        │
└─────────────────────────────────────────────────────────┘
```

### 2.2 Backtester Service Architecture

#### 2.2.1 Core Components

```java
public class BacktestEngine {
    private final HistoricalDataLoader dataLoader;
    private final StrategySimulator simulator;
    private final MetricsCalculator metricsCalculator;
    private final List<Trade> trades = new ArrayList<>();
    
    public BacktestResult runBacktest(BacktestConfig config) {
        // 1. Load historical data from PostgreSQL
        List<AuditEvent> historicalEvents = dataLoader.loadEvents(
            config.getStartDate(),
            config.getEndDate(),
            config.getProducts()
        );
        
        // 2. Replay events through strategy
        SimulatedPortfolio portfolio = new SimulatedPortfolio(config.getInitialCapital());
        for (AuditEvent event : historicalEvents) {
            simulator.processEvent(event, portfolio, config.getStrategy());
        }
        
        // 3. Calculate metrics
        BacktestMetrics metrics = metricsCalculator.calculate(
            trades,
            portfolio,
            config.getInitialCapital()
        );
        
        return new BacktestResult(metrics, trades, portfolio);
    }
}

public class BacktestConfig {
    private LocalDate startDate;
    private LocalDate endDate;
    private List<String> products;
    private double initialCapital;
    private TradingStrategy strategy;
    private double transactionCost;    // Basis points
}
```

#### 2.2.2 Financial Metrics Specification

```java
public class BacktestMetrics {
    // Basic metrics
    private double totalReturn;                    // (Final Value - Initial) / Initial
    private double annualizedReturn;               // Total Return ^ (365 / days)
    private long totalTrades;
    private long winningTrades;
    private long losingTrades;
    
    // Risk metrics
    private double maxDrawdown;                    // Largest peak-to-trough decline
    private double sharpeRatio;                    // (Return - RiskFreeRate) / Volatility
    private double volatility;                     // Standard deviation of returns
    private double sortino Ratio;                  // Like Sharpe but only downside volatility
    private double calmarRatio;                    // Annual Return / Max Drawdown
    
    // Trade metrics
    private double winRate;                        // Winning Trades / Total Trades
    private double profitFactor;                   // Gross Profit / Gross Loss
    private double averageWin;
    private double averageLoss;
    private double expectancy;                     // (Win% * Avg Win) - (Loss% * Avg Loss)
    
    // Execution metrics
    private double averageSlippage;
    private double commissionPaid;
    private long largestWin;
    private long largestLoss;
    private int consecutiveWins;
    private int consecutiveLosses;
    
    public void calculate(List<Trade> trades, double initialCapital) {
        // Implementation details in next section
    }
}
```

#### 2.2.3 Metrics Calculation Logic

```java
public class MetricsCalculator {
    
    public BacktestMetrics calculate(List<Trade> trades, 
                                     SimulatedPortfolio portfolio,
                                     double initialCapital) {
        BacktestMetrics metrics = new BacktestMetrics();
        
        // 1. Basic Returns
        double finalValue = portfolio.getEquity();
        metrics.totalReturn = (finalValue - initialCapital) / initialCapital;
        metrics.annualizedReturn = Math.pow(1 + metrics.totalReturn, 365.0 / getDays()) - 1;
        
        // 2. Trade Analysis
        metrics.totalTrades = trades.size();
        List<Double> returns = new ArrayList<>();
        double cumulativeProfit = 0;
        double grossProfit = 0;
        double grossLoss = 0;
        
        for (Trade trade : trades) {
            double tradeReturn = (trade.getExitPrice() - trade.getEntryPrice()) 
                                / trade.getEntryPrice();
            returns.add(tradeReturn);
            
            if (tradeReturn > 0) {
                metrics.winningTrades++;
                grossProfit += trade.getProfit();
            } else {
                metrics.losingTrades++;
                grossLoss += Math.abs(trade.getProfit());
            }
        }
        
        // 3. Risk Metrics
        metrics.volatility = calculateStandardDeviation(returns);
        metrics.sharpeRatio = calculateSharpeRatio(returns, metrics.volatility);
        metrics.maxDrawdown = calculateMaxDrawdown(trades);
        metrics.sortino Ratio = calculateSortinoRatio(returns, metrics.volatility);
        
        // 4. Trade Quality Metrics
        metrics.winRate = metrics.winningTrades / (double) metrics.totalTrades;
        metrics.profitFactor = grossProfit / Math.max(1.0, grossLoss);
        metrics.expectancy = (metrics.winRate * metrics.averageWin) 
                           - ((1 - metrics.winRate) * metrics.averageLoss);
        
        return metrics;
    }
    
    private double calculateMaxDrawdown(List<Trade> trades) {
        double peak = 0;
        double maxDD = 0;
        double runningEquity = 0;
        
        for (Trade trade : trades) {
            runningEquity += trade.getProfit();
            if (runningEquity > peak) {
                peak = runningEquity;
            }
            double drawdown = peak - runningEquity;
            if (drawdown > maxDD) {
                maxDD = drawdown;
            }
        }
        
        return maxDD;
    }
    
    private double calculateSharpeRatio(List<Double> returns, double volatility) {
        double avgReturn = returns.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double riskFreeRate = 0.02 / 252; // Assuming 2% annual risk-free rate
        return (avgReturn - riskFreeRate) / volatility;
    }
}
```

#### 2.2.4 Historical Data Loader

```java
public class HistoricalDataLoader {
    private final DatabaseManager dbManager;
    
    public List<AuditEvent> loadEvents(LocalDate startDate, 
                                       LocalDate endDate,
                                       List<String> products) {
        String sql = """
            SELECT event_id, event_type, timestamp, user_id, product, 
                   data, hash, prev_hash
            FROM audit_log
            WHERE timestamp BETWEEN ? AND ?
            AND product = ANY(?)
            ORDER BY timestamp ASC
        """;
        
        try (Connection conn = dbManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate.atStartOfDay()));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate.atTime(23, 59, 59)));
            stmt.setArray(3, conn.createArrayOf("varchar", products.toArray()));
            
            List<AuditEvent> events = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    AuditEvent event = AuditEvent.fromResultSet(rs);
                    events.add(event);
                }
            }
            
            return events;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load historical data", e);
        }
    }
}
```

---

### 2.3 Real-Time Algorithmic Trading API

#### 2.3.1 gRPC Service Definition

```protobuf
// trading_api.proto

syntax = "proto3";

package protrade.trading.api;

service TradingService {
    // Order submission
    rpc SubmitOrder(OrderRequest) returns (OrderResponse);
    rpc CancelOrder(CancelRequest) returns (CancelResponse);
    
    // Market data
    rpc GetTopOfBook(TOBRequest) returns (TOBResponse);
    rpc SubscribeMarketData(stream MarketDataRequest) returns (stream MarketDataUpdate);
    
    // Portfolio queries
    rpc GetPortfolioState(EmptyRequest) returns (PortfolioState);
}

message OrderRequest {
    string symbol = 1;
    string side = 2;              // BUY or SELL
    int64 quantity = 3;
    double price = 4;
    string order_type = 5;        // LIMIT, MARKET, FOK, OCO
    string client_order_id = 6;
    repeated OrderAttribute attributes = 7;
}

message OrderAttribute {
    string key = 1;
    string value = 2;
}

message OrderResponse {
    bool success = 1;
    string order_id = 2;
    string status = 3;
    string message = 4;
}

message TOBRequest {
    string symbol = 1;
}

message TOBResponse {
    string symbol = 1;
    double bid_price = 2;
    int64 bid_quantity = 3;
    double ask_price = 4;
    int64 ask_quantity = 5;
    int64 timestamp = 6;
}

message MarketDataUpdate {
    string symbol = 1;
    double last_price = 2;
    int64 last_quantity = 3;
    int64 volume = 4;
    int64 timestamp = 5;
}
```

#### 2.3.2 gRPC Service Implementation

```java
public class TradingServiceImpl extends TradingServiceGrpc.TradingServiceImplBase {
    private final ProductManager productManager;
    private final OrderExecutor orderExecutor;
    private final MarketDataBroadcaster broadcaster;
    
    @Override
    public void submitOrder(OrderRequest request, StreamObserver<OrderResponse> responseObserver) {
        try {
            // 1. Create order object from request
            Order order = new Order.Builder()
                .symbol(request.getSymbol())
                .side(request.getSide())
                .quantity(request.getQuantity())
                .price(request.getPrice())
                .orderType(request.getOrderType())
                .clientOrderId(request.getClientOrderId())
                .build();
            
            // 2. Submit to order executor
            String orderId = orderExecutor.submitOrder(order);
            
            // 3. Return response
            OrderResponse response = OrderResponse.newBuilder()
                .setSuccess(true)
                .setOrderId(orderId)
                .setStatus("ACCEPTED")
                .build();
            
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            
        } catch (Exception e) {
            OrderResponse errorResponse = OrderResponse.newBuilder()
                .setSuccess(false)
                .setMessage(e.getMessage())
                .build();
            
            responseObserver.onNext(errorResponse);
            responseObserver.onCompleted();
        }
    }
    
    @Override
    public void subscribeMarketData(StreamObserver<MarketDataUpdate> responseObserver) {
        // Add subscriber to broadcaster
        broadcaster.addSubscriber(new MarketDataSubscriber() {
            @Override
            public void onUpdate(MarketDataUpdate update) {
                responseObserver.onNext(update);
            }
        });
    }
}
```

#### 2.3.3 WebSocket Market Data Stream

```java
public class WebSocketMarketDataHandler {
    
    @WebSocket
    public class MarketDataEndpoint {
        
        @OnConnect
        public void onConnect(Session session) {
            // Store session and add to broadcaster
            MarketDataBroadcaster.getInstance().addSession(session);
        }
        
        @OnMessage
        public void onMessage(Session session, String message) {
            // Parse subscription request
            JsonObject request = JsonParser.parseString(message).getAsJsonObject();
            String symbol = request.get("symbol").getAsString();
            
            // Add filter
            MarketDataBroadcaster.getInstance().addFilter(session, symbol);
        }
        
        @OnClose
        public void onClose(Session session, CloseReason reason) {
            MarketDataBroadcaster.getInstance().removeSession(session);
        }
    }
}

public class MarketDataBroadcaster {
    private static final MarketDataBroadcaster instance = new MarketDataBroadcaster();
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private final Map<Session, Set<String>> filters = new ConcurrentHashMap<>();
    
    public void broadcast(String symbol, MarketUpdate update) {
        String message = new Gson().toJson(update);
        
        for (Session session : sessions) {
            Set<String> subscribedSymbols = filters.getOrDefault(session, Set.of());
            if (subscribedSymbols.contains(symbol)) {
                try {
                    session.getRemote().sendStringByFuture(message);
                } catch (IOException e) {
                    sessions.remove(session);
                }
            }
        }
    }
}
```

---

## Part 3: Latency Monitoring & Resilience

### 3.1 Microsecond Latency Instrumentation

#### 3.1.1 Critical Path Timestamp Instrumentation

```java
public class LatencyInstrumentation {
    
    /**
     * Critical timestamps to capture (in nanosecond precision):
     * 
     * T0: Order arrival at API (message timestamp)
     * T1: Order deserialization complete
     * T2: Order validation complete
     * T3: Order enters ProductBook lock (write lock acquired)
     * T4: Order matching begins
     * T5: Order matching complete (fills determined)
     * T6: Trade execution begins
     * T7: Trade execution complete
     * T8: Audit log write begins
     * T9: Audit log write complete
     * T10: Response sent to client
     */
    
    public class OrderProcessingTimeline {
        public long t0_arrival;           // Message receives timestamp
        public long t1_deserialized;      // After JSON parsing
        public long t2_validated;         // After business logic validation
        public long t3_lockAcquired;      // Write lock obtained
        public long t4_matchingBegins;    // Matching engine invoked
        public long t5_matchingComplete;  // All fills determined
        public long t6_executionBegins;   // Trade execution starts
        public long t7_executionDone;     // Trade execution completes
        public long t8_auditBegins;       // Audit log write starts
        public long t9_auditComplete;     // Audit log write done
        public long t10_responseSent;     // Response sent to client
        
        public long getEndToEndLatency() {
            return t10_responseSent - t0_arrival;
        }
        
        public long getMatchingLatency() {
            return t5_matchingComplete - t4_matchingBegins;
        }
        
        public long getAuditingLatency() {
            return t9_auditComplete - t8_auditBegins;
        }
        
        public void logTimeline() {
            Map<String, Long> phases = new LinkedHashMap<>();
            phases.put("Deserialization", t1_deserialized - t0_arrival);
            phases.put("Validation", t2_validated - t1_deserialized);
            phases.put("Lock Acquisition", t3_lockAcquired - t2_validated);
            phases.put("Matching", t5_matchingComplete - t4_matchingBegins);
            phases.put("Execution", t7_executionDone - t6_executionBegins);
            phases.put("Auditing", t9_auditComplete - t8_auditBegins);
            phases.put("Response", t10_responseSent - t9_auditComplete);
            
            logger.info("Order Processing Latency Breakdown:");
            phases.forEach((phase, nanos) -> {
                logger.info("  {}: {} micros", phase, nanos / 1000.0);
            });
        }
    }
}
```

#### 3.1.2 Instrumentation Integration

```java
public class InstrumentedProductBook extends ProductBook {
    
    @Override
    public void addOrder(Order order) throws InvalidPriceOperation {
        OrderProcessingTimeline timeline = new OrderProcessingTimeline();
        timeline.t0_arrival = System.nanoTime();  // Message arrival
        
        try {
            // Validation
            timeline.t2_validated = System.nanoTime();
            validateOrder(order);
            timeline.t2_validated = System.nanoTime();
            
            // Lock acquisition
            timeline.t3_lockAcquired = System.nanoTime();
            lock.writeLock().lock();
            timeline.t3_lockAcquired = System.nanoTime();
            
            try {
                // Matching
                timeline.t4_matchingBegins = System.nanoTime();
                executeMatching(order);
                timeline.t5_matchingComplete = System.nanoTime();
                
                // Execution
                timeline.t6_executionBegins = System.nanoTime();
                persistOrder(order);
                timeline.t7_executionDone = System.nanoTime();
                
                // Auditing
                timeline.t8_auditBegins = System.nanoTime();
                logToAudit(order);
                timeline.t9_auditComplete = System.nanoTime();
                
            } finally {
                lock.writeLock().unlock();
            }
            
            timeline.t10_responseSent = System.nanoTime();
            
            // Record for monitoring
            LatencyMonitor.getInstance().recordTimeline(timeline);
            
            // Log if exceeds threshold
            if (timeline.getEndToEndLatency() > 1_000_000) { // 1ms threshold
                logger.warn("High latency detected: {} micros", 
                    timeline.getEndToEndLatency() / 1000.0);
                timeline.logTimeline();
            }
            
        } finally {
            // Ensure lock is released
        }
    }
}
```

#### 3.1.3 Latency Monitoring Service

```java
public class LatencyMonitor {
    private static final LatencyMonitor instance = new LatencyMonitor();
    private final Queue<OrderProcessingTimeline> recentTimelines = 
        new ConcurrentLinkedQueue<>();
    private final DescriptiveStatistics stats = new DescriptiveStatistics(10000);
    
    public void recordTimeline(OrderProcessingTimeline timeline) {
        long latency = timeline.getEndToEndLatency();
        stats.addValue(latency);
        recentTimelines.offer(timeline);
        
        // Keep only recent 10k timelines
        if (recentTimelines.size() > 10000) {
            recentTimelines.poll();
        }
    }
    
    public LatencyStats getStats() {
        return new LatencyStats(
            stats.getMin(),
            stats.getMax(),
            stats.getMean(),
            stats.getPercentile(50),  // P50
            stats.getPercentile(95),  // P95
            stats.getPercentile(99),  // P99
            stats.getPercentile(99.9) // P99.9
        );
    }
    
    public void exportMetrics() {
        LatencyStats stats = getStats();
        logger.info("Latency Metrics (nanoseconds):");
        logger.info("  Min: {} nanos", stats.min);
        logger.info("  P50: {} nanos", stats.p50);
        logger.info("  P95: {} nanos", stats.p95);
        logger.info("  P99: {} nanos", stats.p99);
        logger.info("  P99.9: {} nanos", stats.p99_9);
        logger.info("  Max: {} nanos", stats.max);
    }
}

public class LatencyStats {
    public long min, max, mean, p50, p95, p99, p99_9;
    
    public LatencyStats(long min, long max, double mean, 
                       double p50, double p95, double p99, double p99_9) {
        this.min = min;
        this.max = max;
        this.mean = (long) mean;
        this.p50 = (long) p50;
        this.p95 = (long) p95;
        this.p99 = (long) p99;
        this.p99_9 = (long) p99_9;
    }
}
```

---

### 3.2 Clustering & Hot-Swap Failover Architecture

#### 3.2.1 High-Level Architecture

```
┌─────────────────┐         ┌─────────────────┐
│  Primary Engine │         │ Secondary Engine│
│   (ACTIVE)      │         │   (STANDBY)     │
└────────┬────────┘         └────────┬────────┘
         │                           │
         └───────────┬───────────────┘
                     │
              ┌──────▼──────┐
              │  Kafka Cluster│
              │  Order Topic  │
              │  State Topic  │
              └──────────────┘
                     │
         ┌───────────┼───────────┐
         │           │           │
     ┌───▼──┐   ┌────▼────┐  ┌──▼────┐
     │Client│   │Monitor  │  │Backup │
     │  1   │   │Process  │  │ Node  │
     └──────┘   └─────────┘  └───────┘
```

#### 3.2.2 Apache Kafka Integration

```java
/**
 * Kafka serves two critical purposes:
 * 1. Persistent, ordered order queue (Order Topic)
 * 2. State replication for failover recovery (State Topic)
 */

public class KafkaOrderQueue {
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private static final String ORDER_TOPIC = "trading-orders";
    private static final String STATE_TOPIC = "engine-state";
    
    public KafkaOrderQueue() {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
            StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
            StringSerializer.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        
        this.producer = new KafkaProducer<>(producerProps);
    }
    
    /**
     * Producer: Write incoming orders to Kafka
     */
    public void publishOrder(Order order) {
        String orderJson = new Gson().toJson(order);
        ProducerRecord<String, String> record = 
            new ProducerRecord<>(ORDER_TOPIC, order.getOrderId(), orderJson);
        
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to publish order to Kafka", exception);
            } else {
                logger.debug("Order published to Kafka: partition={}, offset={}", 
                    metadata.partition(), metadata.offset());
            }
        });
    }
    
    /**
     * Consumer: Backup engine subscribes to orders
     */
    public void subscribeToOrders(OrderProcessor processor) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "backup-engine");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(ORDER_TOPIC));
        
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    Order order = new Gson().fromJson(record.value(), Order.class);
                    processor.processOrder(order);
                    consumer.commitSync();
                }
            }
        }).start();
    }
}
```

#### 3.2.3 State Replication

```java
public class EngineStateReplica {
    private final KafkaProducer<String, String> producer;
    private static final String STATE_TOPIC = "engine-state";
    
    /**
     * Publish order book snapshot to Kafka periodically
     * Backup engine uses this to reconstruct current state
     */
    public void replicateState(ProductBook book) {
        EngineSnapshot snapshot = new EngineSnapshot(
            System.currentTimeMillis(),
            book.getOrderBook(),
            book.getFilledTrades(),
            book.getExecutedVolume()
        );
        
        String stateJson = new Gson().toJson(snapshot);
        ProducerRecord<String, String> record = 
            new ProducerRecord<>(STATE_TOPIC, "SNAPSHOT", stateJson);
        
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Failed to replicate state", exception);
            }
        });
    }
    
    public static class EngineSnapshot {
        public long timestamp;
        public List<Order> openOrders;
        public List<Trade> completedTrades;
        public Map<String, Long> executedVolume;
        
        public EngineSnapshot(long timestamp, List<Order> openOrders, 
                             List<Trade> trades, Map<String, Long> volume) {
            this.timestamp = timestamp;
            this.openOrders = openOrders;
            this.completedTrades = trades;
            this.executedVolume = volume;
        }
    }
}
```

#### 3.2.4 Hot-Swap Failover Logic

```java
public class FailoverManager {
    private final PrimaryEngine primaryEngine;
    private final SecondaryEngine secondaryEngine;
    private final FailoverMonitor monitor;
    private volatile EngineMode activeMode = EngineMode.PRIMARY;
    
    public enum EngineMode {
        PRIMARY, SECONDARY
    }
    
    /**
     * Failover sequence:
     * 1. Detect primary failure (health check timeout)
     * 2. Pause order acceptance
     * 3. Load latest state snapshot from Kafka
     * 4. Activate secondary engine
     * 5. Resume order processing
     */
    public void executeFailover() {
        logger.info("FAILOVER: Initiating secondary engine activation");
        
        // 1. Stop accepting orders on primary
        primaryEngine.pause();
        
        // 2. Load latest snapshot from Kafka
        EngineSnapshot snapshot = loadLatestSnapshot();
        if (snapshot == null) {
            logger.error("FAILOVER: Failed to load snapshot");
            return;
        }
        
        // 3. Restore secondary engine state
        secondaryEngine.restoreFromSnapshot(snapshot);
        
        // 4. Switch to secondary
        activeMode = EngineMode.SECONDARY;
        secondaryEngine.resume();
        
        logger.info("FAILOVER: Secondary engine is now ACTIVE");
        
        // 5. Attempt to restore primary
        new Thread(() -> {
            try {
                Thread.sleep(30000); // Wait 30 seconds
                if (primaryEngine.healthCheck()) {
                    logger.info("FAILOVER: Primary engine restored, initiating switchback");
                    executeFailback();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    /**
     * Switchback to primary engine
     */
    private void executeFailback() {
        // 1. Sync secondary state to primary
        EngineSnapshot currentState = secondaryEngine.captureState();
        primaryEngine.restoreFromSnapshot(currentState);
        
        // 2. Switch active mode
        activeMode = EngineMode.PRIMARY;
        primaryEngine.resume();
        
        // 3. Pause secondary
        secondaryEngine.pause();
        
        logger.info("FAILOVER: Switchback to primary engine complete");
    }
    
    private EngineSnapshot loadLatestSnapshot() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "failover-manager");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("engine-state"));
        
        EngineSnapshot latest = null;
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        
        for (ConsumerRecord<String, String> record : records) {
            latest = new Gson().fromJson(record.value(), EngineSnapshot.class);
        }
        
        consumer.close();
        return latest;
    }
}

public class FailoverMonitor extends Thread {
    private final PrimaryEngine primaryEngine;
    private final FailoverManager failoverManager;
    private static final long HEALTH_CHECK_INTERVAL = 1000; // 1 second
    private static final long HEALTH_CHECK_TIMEOUT = 5000;  // 5 seconds
    
    @Override
    public void run() {
        while (true) {
            try {
                if (!primaryEngine.healthCheck(HEALTH_CHECK_TIMEOUT)) {
                    logger.error("Primary engine health check failed");
                    failoverManager.executeFailover();
                    return; // Exit monitoring, secondary is now active
                }
                
                Thread.sleep(HEALTH_CHECK_INTERVAL);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
```

#### 3.2.5 Integration with Existing Engine

```java
public class ResilientProductBook extends ProductBook {
    private final FailoverManager failoverManager;
    private final KafkaOrderQueue kafkaQueue;
    
    @Override
    public void addOrder(Order order) throws InvalidPriceOperation {
        // 1. Publish to Kafka for durability and backup consumption
        kafkaQueue.publishOrder(order);
        
        // 2. Process order normally
        super.addOrder(order);
        
        // 3. Replicate state periodically (every 100 orders)
        if (Math.random() < 0.01) { // 1% sampling for state replication
            EngineStateReplica replica = new EngineStateReplica();
            replica.replicateState(this);
        }
    }
}
```

---

## Implementation Timeline & Priorities

### Phase 1: Advanced Order Types (Weeks 1-2)
- [ ] Implement OCO order linking infrastructure
- [ ] Implement FOK validation logic
- [ ] Implement STP prevention mechanism
- [ ] Integration tests for all three features
- [ ] Performance benchmarking

### Phase 2: Backtesting Module (Weeks 3-4)
- [ ] Historical data loader from PostgreSQL
- [ ] Backtesting engine skeleton
- [ ] Metrics calculator (P&L, Sharpe, Drawdown)
- [ ] Backtesting CLI interface
- [ ] Sample strategies for testing

### Phase 3: Real-Time APIs (Weeks 5-6)
- [ ] gRPC service definition and implementation
- [ ] WebSocket market data broadcaster
- [ ] Sample algorithmic trading client
- [ ] Load testing and optimization
- [ ] Documentation

### Phase 4: Latency & Resilience (Weeks 7-8)
- [ ] Instrumentation of critical paths
- [ ] Latency monitoring dashboard
- [ ] Kafka integration (order queue + state replication)
- [ ] Failover/failback logic
- [ ] Chaos engineering tests (simulate failures)

---

## Success Metrics

| Metric | Target | Measurement |
|--------|--------|------------|
| Order-to-Execution Latency (P99) | < 100 µs | LatencyMonitor percentiles |
| Matching Latency | < 50 µs | Timeline T4-T5 |
| Backtest Throughput | > 1M events/sec | Benchmark test |
| Failover Time | < 5 seconds | FailoverManager tests |
| Data Loss on Failover | 0 | Kafka durability verification |
| OCO Success Rate | 99.99% | Integration tests |
| FOK Accuracy | 100% | Unit tests |

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| Kafka performance bottleneck | Load test early; consider Chronicle Queue alternative |
| State synchronization lag | Implement heartbeat-based health checks; < 100ms tolerance |
| Latency instrumentation overhead | Use sampling (1-5%); measure overhead separately |
| Failover test coverage | Chaos engineering suite; monthly failover drills |

