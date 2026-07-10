package exchange.persistence;

import Price.Price;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.lmax.disruptor.EventHandler;
import exchange.dispatch.EventListener;
import exchange.dispatch.RingBufferEvent;
import exchange.model.ExchangeEvent;
import exchange.model.OrderAccepted;
import exchange.model.OrderCancelled;
import exchange.model.OrderExecuted;
import exchange.model.OrderRejected;
import exchange.model.OrderRestated;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class PostgresAuditService implements EventHandler<RingBufferEvent>, EventListener, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(PostgresAuditService.class);
    private static final String BATCH_EVENT_TYPE = "EXCHANGE_EVENT_BATCH";
    private static final String INSERT_SQL = """
            INSERT INTO exchange_events (sequence_id, inbound_timestamp, event_type, symbol, payload)
            VALUES (?, ?, ?, ?, ?::jsonb)
            ON CONFLICT (sequence_id) DO NOTHING
            """;
    private static final String REPLAY_SQL = """
            SELECT sequence_id, payload
            FROM exchange_events
            ORDER BY sequence_id ASC
            """;

    private final DataSource dataSource;
    private final Gson gson;
    private final int jdbcBatchSize;
    private final Map<Long, SequenceAccumulator> pendingBySequence = new HashMap<>();
    private final ArrayList<EventBatch> readyBatches = new ArrayList<>();

    public PostgresAuditService(DataSource dataSource) {
        this(dataSource, 256);
    }

    public PostgresAuditService(DataSource dataSource, int jdbcBatchSize) {
        this.dataSource = dataSource;
        this.jdbcBatchSize = jdbcBatchSize;
        this.gson = new GsonBuilder()
                .registerTypeAdapter(Instant.class, new InstantAdapter())
                .registerTypeAdapter(Price.class, new PriceAdapter())
                .create();
    }

    public PostgresAuditService(DataSource dataSource, int jdbcBatchSize, java.time.Duration ignoredFlushInterval,
            int ignoredQueueCapacity) {
        this(dataSource, jdbcBatchSize);
    }

    @Override
    public void onEvent(RingBufferEvent event, long sequence, boolean endOfBatch) {
        SequenceAccumulator accumulator = pendingBySequence.computeIfAbsent(event.getSequenceNumber(),
                ignored -> new SequenceAccumulator(event.getSequenceNumber(), epochNanos(event.getEventTimestamp()),
                        event.getSymbol(), event.getBatchSize()));
        accumulator.add(event.toImmutableEvent());
        if (accumulator.isComplete()) {
            pendingBySequence.remove(event.getSequenceNumber());
            readyBatches.add(accumulator.toEventBatch(gson));
        }
        if (readyBatches.size() >= jdbcBatchSize || (endOfBatch && !readyBatches.isEmpty())) {
            flushReadyBatches();
        }
    }

    @Override
    public void onEvents(List<ExchangeEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        readyBatches.add(EventBatch.from(events, gson));
        if (readyBatches.size() >= jdbcBatchSize) {
            flushReadyBatches();
        }
    }

    public ReplayResult replayEvents() throws SQLException {
        ArrayList<List<ExchangeEvent>> replayBatches = new ArrayList<>();
        long maxSequenceId = 0;
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(REPLAY_SQL)) {
            while (resultSet.next()) {
                long sequenceId = resultSet.getLong("sequence_id");
                EventBatch batch = deserializeBatch(resultSet.getString("payload"));
                replayBatches.add(batch.events());
                maxSequenceId = Math.max(maxSequenceId, sequenceId);
            }
        }
        return new ReplayResult(List.copyOf(replayBatches), maxSequenceId);
    }

    @Override
    public void close() {
        if (!pendingBySequence.isEmpty()) {
            for (SequenceAccumulator accumulator : pendingBySequence.values()) {
                readyBatches.add(accumulator.toEventBatch(gson));
            }
            pendingBySequence.clear();
        }
        flushReadyBatches();
    }

    private void flushReadyBatches() {
        if (readyBatches.isEmpty()) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
                PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
            for (EventBatch batch : readyBatches) {
                statement.setLong(1, batch.sequenceId());
                statement.setLong(2, batch.inboundTimestamp());
                statement.setString(3, batch.eventType());
                statement.setString(4, batch.symbol());
                statement.setString(5, batch.payloadJson());
                statement.addBatch();
            }
            statement.executeBatch();
        } catch (SQLException e) {
            logger.error("Failed to persist exchange event batch", e);
        } finally {
            readyBatches.clear();
        }
    }

    private EventBatch deserializeBatch(String json) {
        JsonObject payload = gson.fromJson(json, JsonObject.class);
        JsonArray eventPayloads = payload.getAsJsonArray("events");
        ArrayList<ExchangeEvent> events = new ArrayList<>(eventPayloads.size());
        for (JsonElement element : eventPayloads) {
            JsonObject serialized = element.getAsJsonObject();
            events.add(deserializeEvent(serialized.get("type").getAsString(), serialized.get("payload")));
        }
        return EventBatch.from(List.copyOf(events), json, gson);
    }

    private ExchangeEvent deserializeEvent(String eventType, JsonElement payload) {
        return switch (eventType) {
            case "OrderAccepted" -> gson.fromJson(payload, OrderAccepted.class);
            case "OrderRejected" -> gson.fromJson(payload, OrderRejected.class);
            case "OrderExecuted" -> gson.fromJson(payload, OrderExecuted.class);
            case "OrderCancelled" -> gson.fromJson(payload, OrderCancelled.class);
            case "OrderRestated" -> gson.fromJson(payload, OrderRestated.class);
            default -> throw new IllegalArgumentException("Unsupported exchange event type: " + eventType);
        };
    }

    public record ReplayResult(List<List<ExchangeEvent>> batches, long maxSequenceId) {
    }

    public record EventBatch(
            long sequenceId,
            long inboundTimestamp,
            String eventType,
            String symbol,
            List<ExchangeEvent> events,
            String payloadJson) {

        private static EventBatch from(List<ExchangeEvent> events, Gson gson) {
            return from(events, null, gson);
        }

        private static EventBatch from(List<ExchangeEvent> events, String payloadJson, Gson gson) {
            if (events.isEmpty()) {
                throw new IllegalArgumentException("Cannot persist an empty event batch");
            }
            ExchangeEvent first = events.getFirst();
            String eventType = events.size() == 1 ? first.getClass().getSimpleName() : BATCH_EVENT_TYPE;
            String json = payloadJson == null ? serialize(events, eventType, gson) : payloadJson;
            return new EventBatch(first.sequenceNumber(), epochNanos(first.eventTimestamp()), eventType,
                    first.symbol(), List.copyOf(events), json);
        }

        private static String serialize(List<ExchangeEvent> events, String eventType, Gson gson) {
            JsonObject payload = new JsonObject();
            ExchangeEvent first = events.getFirst();
            payload.addProperty("sequenceId", first.sequenceNumber());
            payload.addProperty("inboundTimestamp", epochNanos(first.eventTimestamp()));
            payload.addProperty("eventType", eventType);
            payload.addProperty("symbol", first.symbol());

            JsonArray eventPayloads = new JsonArray();
            for (ExchangeEvent event : events) {
                JsonObject serialized = new JsonObject();
                serialized.addProperty("type", event.getClass().getSimpleName());
                serialized.add("payload", gson.toJsonTree(event));
                eventPayloads.add(serialized);
            }
            payload.add("events", eventPayloads);
            return gson.toJson(payload);
        }
    }

    private static final class SequenceAccumulator {
        private final long sequenceId;
        private final long inboundTimestamp;
        private final String symbol;
        private final int expectedEvents;
        private final ArrayList<ExchangeEvent> events = new ArrayList<>();

        private SequenceAccumulator(long sequenceId, long inboundTimestamp, String symbol, int expectedEvents) {
            this.sequenceId = sequenceId;
            this.inboundTimestamp = inboundTimestamp;
            this.symbol = symbol;
            this.expectedEvents = expectedEvents;
        }

        private void add(ExchangeEvent event) {
            events.add(event);
        }

        private boolean isComplete() {
            return events.size() >= expectedEvents;
        }

        private EventBatch toEventBatch(Gson gson) {
            if (events.isEmpty()) {
                throw new IllegalStateException("Cannot persist an empty sequence accumulator");
            }
            String eventType = events.size() == 1 ? events.getFirst().getClass().getSimpleName() : BATCH_EVENT_TYPE;
            JsonObject payload = new JsonObject();
            payload.addProperty("sequenceId", sequenceId);
            payload.addProperty("inboundTimestamp", inboundTimestamp);
            payload.addProperty("eventType", eventType);
            payload.addProperty("symbol", symbol);

            JsonArray eventPayloads = new JsonArray();
            for (ExchangeEvent event : events) {
                JsonObject serialized = new JsonObject();
                serialized.addProperty("type", event.getClass().getSimpleName());
                serialized.add("payload", gson.toJsonTree(event));
                eventPayloads.add(serialized);
            }
            payload.add("events", eventPayloads);
            return new EventBatch(sequenceId, inboundTimestamp, eventType, symbol, List.copyOf(events),
                    gson.toJson(payload));
        }
    }

    private static long epochNanos(Instant instant) {
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
    }

    private static final class InstantAdapter implements JsonSerializer<Instant>, JsonDeserializer<Instant> {
        @Override
        public JsonElement serialize(Instant source, Type typeOfSource, JsonSerializationContext context) {
            return new JsonPrimitive(source.toString());
        }

        @Override
        public Instant deserialize(JsonElement json, Type typeOfTarget, JsonDeserializationContext context)
                throws JsonParseException {
            return Instant.parse(json.getAsString());
        }
    }

    private static final class PriceAdapter implements JsonSerializer<Price>, JsonDeserializer<Price> {
        @Override
        public JsonElement serialize(Price source, Type typeOfSource, JsonSerializationContext context) {
            return new JsonPrimitive(source.getCents());
        }

        @Override
        public Price deserialize(JsonElement json, Type typeOfTarget, JsonDeserializationContext context)
                throws JsonParseException {
            return new Price(json.getAsLong());
        }
    }
}
