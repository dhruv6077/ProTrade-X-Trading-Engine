package persistence;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka state replication service.
 * Publishes periodic engine snapshots for backup/failover purposes.
 */
public class KafkaStateReplicator {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStateReplicator.class);
    private static final KafkaStateReplicator instance = new KafkaStateReplicator();
    
    private static final String STATE_TOPIC = "engine-state";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    
    private volatile EngineStateSnapshot latestSnapshot;
    private final AtomicBoolean replicating = new AtomicBoolean(false);
    
    private KafkaStateReplicator() {
        this.latestSnapshot = new EngineStateSnapshot(System.currentTimeMillis(), 
            new ArrayList<>(), new ArrayList<>(), new ConcurrentHashMap<>(), 0);
    }
    
    public static KafkaStateReplicator getInstance() {
        return instance;
    }
    
    /**
     * Replicate current engine state to Kafka.
     * 
     * @param snapshot The state snapshot to replicate
     */
    public void replicateState(EngineStateSnapshot snapshot) {
        if (snapshot == null) {
            return;
        }
        
        this.latestSnapshot = snapshot;
        
        try {
            org.apache.kafka.clients.producer.KafkaProducer<String, String> producer = 
                new org.apache.kafka.clients.producer.KafkaProducer<>(getProducerConfig());
            
            String snapshotJson = new Gson().toJson(snapshot);
            org.apache.kafka.clients.producer.ProducerRecord<String, String> record = 
                new org.apache.kafka.clients.producer.ProducerRecord<>(STATE_TOPIC, "SNAPSHOT", snapshotJson);
            
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to replicate state snapshot", exception);
                } else {
                    logger.debug("State snapshot replicated to Kafka: {}", snapshot);
                }
            });
            
            producer.flush();
            producer.close();
            
        } catch (Exception e) {
            logger.error("Error replicating state", e);
        }
    }
    
    /**
     * Get the latest replicated state snapshot.
     */
    public EngineStateSnapshot getLatestSnapshot() {
        return latestSnapshot;
    }
    
    /**
     * Load latest state snapshot from Kafka.
     * Used during failover recovery.
     */
    public EngineStateSnapshot loadLatestSnapshotFromKafka() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "failover-reader");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(STATE_TOPIC));
        
        EngineStateSnapshot latest = null;
        
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            
            for (ConsumerRecord<String, String> record : records) {
                latest = new Gson().fromJson(record.value(), EngineStateSnapshot.class);
            }
        } finally {
            consumer.close();
        }
        
        if (latest != null) {
            logger.info("Loaded state snapshot from Kafka: {}", latest);
        }
        
        return latest;
    }
    
    /**
     * Get Kafka producer configuration.
     */
    private Properties getProducerConfig() {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
            org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        return props;
    }
}
