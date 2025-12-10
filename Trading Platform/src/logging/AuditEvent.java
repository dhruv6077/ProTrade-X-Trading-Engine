package logging;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents an audit event in the trading platform.
 * Each event is immutable and contains all necessary information for audit trail.
 */
public class AuditEvent {
    private final String eventId;
    private final AuditEventType eventType;
    private final Instant timestamp;
    private final String userId;
    private final String product;
    private final Map<String, Object> data;
    private String hash;
    private String previousHash;
    
    private static final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .create();
    
    private AuditEvent(Builder builder) {
        this.eventId = UUID.randomUUID().toString();
        this.eventType = builder.eventType;
        this.timestamp = Instant.now();
        this.userId = builder.userId;
        this.product = builder.product;
        this.data = new HashMap<>(builder.data);
    }
    
    public String getEventId() {
        return eventId;
    }
    
    public AuditEventType getEventType() {
        return eventType;
    }
    
    public Instant getTimestamp() {
        return timestamp;
    }
    
    public String getUserId() {
        return userId;
    }
    
    public String getProduct() {
        return product;
    }
    
    public Map<String, Object> getData() {
        return new HashMap<>(data);
    }
    
    public String getHash() {
        return hash;
    }
    
    public void setHash(String hash) {
        this.hash = hash;
    }
    
    public String getPreviousHash() {
        return previousHash;
    }
    
    public void setPreviousHash(String previousHash) {
        this.previousHash = previousHash;
    }
    
    /**
     * Converts the event to JSON format for logging.
     */
    public String toJson() {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("eventId", eventId);
        jsonMap.put("eventType", eventType.name());
        jsonMap.put("timestamp", timestamp.toString());
        jsonMap.put("userId", userId);
        jsonMap.put("product", product);
        jsonMap.put("data", data);
        jsonMap.put("hash", hash);
        jsonMap.put("previousHash", previousHash);
        
        return gson.toJson(jsonMap);
    }
    
    /**
     * Gets the data to be hashed (excludes hash fields).
     */
    public String getDataForHashing() {
        Map<String, Object> hashData = new HashMap<>();
        hashData.put("eventId", eventId);
        hashData.put("eventType", eventType.name());
        hashData.put("timestamp", timestamp.toString());
        hashData.put("userId", userId);
        hashData.put("product", product);
        hashData.put("data", data);
        
        return gson.toJson(hashData);
    }
    
    /**
     * Builder pattern for creating AuditEvent instances.
     */
    public static class Builder {
        private AuditEventType eventType;
        private String userId;
        private String product;
        private final Map<String, Object> data = new HashMap<>();
        
        public Builder eventType(AuditEventType eventType) {
            this.eventType = eventType;
            return this;
        }
        
        public Builder userId(String userId) {
            this.userId = userId;
            return this;
        }
        
        public Builder product(String product) {
            this.product = product;
            return this;
        }
        
        public Builder addData(String key, Object value) {
            this.data.put(key, value);
            return this;
        }
        
        public Builder addAllData(Map<String, Object> data) {
            this.data.putAll(data);
            return this;
        }
        
        public AuditEvent build() {
            if (eventType == null) {
                throw new IllegalStateException("Event type is required");
            }
            return new AuditEvent(this);
        }
    }
}
