package persistence;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import Tradable.Order;

import java.util.Properties;

/**
 * Kafka producer for publishing orders to a persistent queue.
 * Enables order replay and backup engine synchronization.
 */
public class KafkaOrderProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaOrderProducer.class);
    private static final KafkaOrderProducer instance = new KafkaOrderProducer();
    
    private static final String ORDER_TOPIC = "trading-orders";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    
    private KafkaProducer<String, String> producer;
    private boolean initialized;
    
    private KafkaOrderProducer() {
        try {
            initializeProducer();
            initialized = true;
        } catch (Exception e) {
            logger.warn("Kafka producer initialization failed: {}", e.getMessage());
            initialized = false;
        }
    }
    
    public static KafkaOrderProducer getInstance() {
        return instance;
    }
    
    /**
     * Initialize Kafka producer with durability settings.
     */
    private void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Durability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all");              // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE); // Infinite retries
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        
        // Performance settings
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        
        this.producer = new KafkaProducer<>(props);
        logger.info("Kafka order producer initialized for topic: {}", ORDER_TOPIC);
    }
    
    /**
     * Publish an order to the Kafka topic.
     * 
     * @param order The order to publish
     * @return True if successfully published
     */
    public boolean publishOrder(Order order) {
        if (!initialized || producer == null) {
            logger.warn("Kafka producer not initialized");
            return false;
        }
        
        try {
            String orderJson = new Gson().toJson(order);
            ProducerRecord<String, String> record = 
                new ProducerRecord<>(ORDER_TOPIC, order.getId(), orderJson);
            
            // Send asynchronously with callback
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to publish order {} to Kafka: {}", 
                        order.getId(), exception.getMessage());
                } else {
                    logger.debug("Order {} published to Kafka: partition={}, offset={}", 
                        order.getId(), metadata.partition(), metadata.offset());
                }
            });
            
            return true;
            
        } catch (Exception e) {
            logger.error("Error publishing order to Kafka", e);
            return false;
        }
    }
    
    /**
     * Flush any pending messages.
     */
    public void flush() {
        if (producer != null) {
            producer.flush();
        }
    }
    
    /**
     * Close the producer.
     */
    public void close() {
        if (producer != null) {
            producer.close();
            initialized = false;
            logger.info("Kafka order producer closed");
        }
    }
    
    public boolean isInitialized() {
        return initialized;
    }
}
