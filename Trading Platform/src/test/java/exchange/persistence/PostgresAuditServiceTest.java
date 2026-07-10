package exchange.persistence;

import Price.Price;
import exchange.dispatch.RingBufferEvent;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderExecuted;
import exchange.model.OrderState;
import exchange.model.OrderType;
import exchange.model.Side;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresAuditServiceTest {
    private PostgresAuditService service;

    @AfterEach
    void tearDown() {
        if (service != null) {
            service.close();
        }
    }

    @Test
    void persistsSequencedEventsUsingJdbcBatch() throws Exception {
        RecordingDataSource dataSource = new RecordingDataSource();

        service = new PostgresAuditService(dataSource, 1, Duration.ofMillis(5), 32);
        Instant inboundTimestamp = Instant.parse("2026-06-26T12:00:00.123456789Z");
        OrderAccepted accepted = new OrderAccepted(
                42,
                "B-1",
                "BUYER",
                "AAPL",
                new OrderState("B-1", "BUYER", "AAPL", Side.BUY, OrderType.LIMIT,
                        new Price(10_000), 5, 5, 0, 42),
                inboundTimestamp);

        service.onEvents(List.of(accepted));

        assertEquals(1, dataSource.executeBatchCount);
        assertTrue(dataSource.sql.contains("INSERT INTO exchange_events"));
        assertEquals(42L, dataSource.boundLongs.get(1));
        assertEquals(epochNanos(inboundTimestamp), dataSource.boundLongs.get(2));
        assertEquals("OrderAccepted", dataSource.boundStrings.get(3));
        assertEquals("AAPL", dataSource.boundStrings.get(4));
        assertTrue(dataSource.boundStrings.get(5).contains("\"events\""));
        assertEquals(1, dataSource.addBatchCount);
    }

    @Test
    void persistsMultiEventBatch() throws Exception {
        RecordingDataSource dataSource = new RecordingDataSource();

        service = new PostgresAuditService(dataSource, 1, Duration.ofMillis(5), 32);
        Instant inboundTimestamp = Instant.parse("2026-06-26T12:00:00.123456789Z");
        OrderAccepted accepted = new OrderAccepted(
                42, "B-1", "BUYER", "AAPL",
                new OrderState("B-1", "BUYER", "AAPL", Side.BUY, OrderType.LIMIT, new Price(10_000), 5, 5, 0, 42),
                inboundTimestamp);
        OrderExecuted executed = new OrderExecuted(
                42, "B-1", "BUYER", "AAPL", "S-1", "SELLER", Side.BUY,
                new Price(10_000), 5, 0, 5, true, 0, 0, inboundTimestamp);

        service.onEvents(List.of(accepted, executed));

        assertEquals(1, dataSource.executeBatchCount);
        assertEquals(42L, dataSource.boundLongs.get(1));
        assertEquals("EXCHANGE_EVENT_BATCH", dataSource.boundStrings.get(3));
        assertTrue(dataSource.boundStrings.get(5).contains("OrderAccepted"));
        assertTrue(dataSource.boundStrings.get(5).contains("OrderExecuted"));
    }

    @Test
    void accumulatesDisruptorSequenceUntilFinalBatchEventArrives() throws Exception {
        RecordingDataSource dataSource = new RecordingDataSource();
        service = new PostgresAuditService(dataSource, 8, Duration.ofMillis(5), 32);
        Instant inboundTimestamp = Instant.parse("2026-06-26T12:00:00.123456789Z");

        RingBufferEvent acceptedSlot = new RingBufferEvent();
        RingBufferEvent executedSlot = new RingBufferEvent();
        acceptedSlot.copyFrom(new OrderAccepted(
                42, "B-1", "BUYER", "AAPL",
                new OrderState("B-1", "BUYER", "AAPL", Side.BUY, OrderType.LIMIT, new Price(10_000), 5, 5, 0, 42),
                inboundTimestamp), 0, 2);
        executedSlot.copyFrom(new OrderExecuted(
                42, "B-1", "BUYER", "AAPL", "S-1", "SELLER", Side.BUY,
                new Price(10_000), 5, 0, 5, true, 0, 0, inboundTimestamp), 1, 2);

        service.onEvent(acceptedSlot, 1L, false);
        assertEquals(0, dataSource.executeBatchCount);

        service.onEvent(executedSlot, 2L, true);
        assertEquals(1, dataSource.executeBatchCount);
        assertEquals("EXCHANGE_EVENT_BATCH", dataSource.boundStrings.get(3));
    }

    @Test
    void closeFlushesIncompleteSequenceAccumulator() throws Exception {
        RecordingDataSource dataSource = new RecordingDataSource();
        service = new PostgresAuditService(dataSource, 8, Duration.ofHours(1), 32);

        Instant inboundTimestamp = Instant.parse("2026-06-26T12:00:00.123456789Z");
        RingBufferEvent acceptedSlot = new RingBufferEvent();
        acceptedSlot.copyFrom(new OrderAccepted(
                42, "B-1", "BUYER", "AAPL",
                new OrderState("B-1", "BUYER", "AAPL", Side.BUY, OrderType.LIMIT, new Price(10_000), 5, 5, 0, 42),
                inboundTimestamp), 0, 2);

        service.onEvent(acceptedSlot, 1L, false);
        assertEquals(0, dataSource.executeBatchCount);

        service.close();
        service = null;

        assertEquals(1, dataSource.executeBatchCount);
    }

    @Test
    void replaysAndDeserializesCorrectly() throws Exception {
        String jsonPayload = """
                {
                  "sequenceId": 42,
                  "inboundTimestamp": 1000000000,
                  "eventType": "EXCHANGE_EVENT_BATCH",
                  "symbol": "AAPL",
                  "events": [
                    {
                      "type": "OrderAccepted",
                      "payload": {
                        "sequenceNumber": 42,
                        "orderId": "B-1",
                        "clientId": "BUYER",
                        "symbol": "AAPL",
                        "order": {
                          "orderId": "B-1",
                          "clientId": "BUYER",
                          "symbol": "AAPL",
                          "side": "BUY",
                          "orderType": "LIMIT",
                          "price": 10000,
                          "quantity": 5,
                          "leavesQty": 5,
                          "cumQty": 0,
                          "sequenceNumber": 42
                        },
                        "inboundTimestamp": "2026-06-26T12:00:00.123456789Z",
                        "eventTimestamp": "2026-06-26T12:00:00.123456789Z"
                      }
                    }
                  ]
                }
                """;

        RecordingDataSource dataSource = new RecordingDataSource(jsonPayload);
        service = new PostgresAuditService(dataSource);

        PostgresAuditService.ReplayResult result = service.replayEvents();

        assertEquals(42L, result.maxSequenceId());
        assertEquals(1, result.batches().size());

        List<ExchangeEvent> batch = result.batches().get(0);
        assertEquals(1, batch.size());

        OrderAccepted accepted = (OrderAccepted) batch.get(0);
        assertEquals(42L, accepted.sequenceNumber());
        assertEquals("B-1", accepted.orderId());
        assertEquals(10_000, accepted.order().price().getCents());
    }

    private static long epochNanos(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
    }

    private static final class RecordingDataSource implements DataSource {
        private volatile String sql = "";
        private volatile int addBatchCount;
        private volatile int executeBatchCount;
        private volatile boolean blockExecution = false;
        private final String replayPayload;
        private final java.util.Map<Integer, Long> boundLongs = new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.Map<Integer, String> boundStrings = new java.util.concurrent.ConcurrentHashMap<>();

        public RecordingDataSource() {
            this(null);
        }

        public RecordingDataSource(String replayPayload) {
            this.replayPayload = replayPayload;
        }

        @Override
        public Connection getConnection() {
            InvocationHandler handler = (proxy, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                    sql = Objects.toString(args[0]);
                    return preparedStatementProxy();
                } else if ("createStatement".equals(method.getName())) {
                    return statementProxy();
                }
                return defaultReturn(method);
            };
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { Connection.class }, handler);
        }

        private PreparedStatement preparedStatementProxy() {
            InvocationHandler handler = (proxy, method, args) -> {
                switch (method.getName()) {
                    case "setLong" -> boundLongs.put((Integer) args[0], (Long) args[1]);
                    case "setString" -> boundStrings.put((Integer) args[0], (String) args[1]);
                    case "addBatch" -> addBatchCount++;
                    case "executeBatch" -> {
                        while (blockExecution) {
                            Thread.sleep(1);
                        }
                        executeBatchCount++;
                        return new int[] { 1 };
                    }
                    default -> {
                        return defaultReturn(method);
                    }
                }
                return null;
            };
            return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { PreparedStatement.class }, handler);
        }

        private Statement statementProxy() {
            InvocationHandler handler = (proxy, method, args) -> {
                if ("executeQuery".equals(method.getName())) {
                    return resultSetProxy();
                }
                return defaultReturn(method);
            };
            return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { Statement.class }, handler);
        }

        private ResultSet resultSetProxy() {
            InvocationHandler handler = new InvocationHandler() {
                private boolean hasNext = replayPayload != null;

                @Override
                public Object invoke(Object proxy, Method method, Object[] args) {
                    switch (method.getName()) {
                        case "next" -> {
                            boolean res = hasNext;
                            hasNext = false;
                            return res;
                        }
                        case "getLong" -> {
                            return 42L;
                        }
                        case "getString" -> {
                            return replayPayload;
                        }
                        default -> {
                            return defaultReturn(method);
                        }
                    }

                }
            };
            return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { ResultSet.class }, handler);
        }

        private Object defaultReturn(Method method) {
            Class<?> returnType = method.getReturnType();
            if (returnType == boolean.class) {
                return false;
            }
            if (returnType == byte.class || returnType == short.class || returnType == int.class
                    || returnType == long.class || returnType == float.class || returnType == double.class) {
                return 0;
            }
            return null;
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
