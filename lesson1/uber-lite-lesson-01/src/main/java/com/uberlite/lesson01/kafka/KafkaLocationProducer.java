package com.uberlite.lesson01.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.lesson01.model.LocationEvent;
import com.uberlite.lesson01.model.Metrics;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaLocationProducer {
    private final KafkaProducer<String, String> producer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final List<Long> latencies = new ArrayList<>();
    private static final String TOPIC = "location-updates";

    public KafkaLocationProducer(String bootstrapServers) {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Performance tuning
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64MB
        
        this.producer = new KafkaProducer<>(props);
    }

    public void sendLocation(LocationEvent event) {
        var startTime = System.nanoTime();
        try {
            var json = objectMapper.writeValueAsString(event);
            var record = new ProducerRecord<>(TOPIC, event.driverId(), json);
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    var latencyNs = System.nanoTime() - startTime;
                    synchronized (latencies) {
                        latencies.add(latencyNs);
                    }
                    successCount.incrementAndGet();
                } else {
                    failureCount.incrementAndGet();
                }
            });
        } catch (Exception e) {
            failureCount.incrementAndGet();
        }
    }

    public Metrics getMetrics() {
        producer.flush();
        
        List<Long> snapshot;
        synchronized (latencies) {
            snapshot = new ArrayList<>(latencies);
            latencies.clear();
        }
        
        if (snapshot.isEmpty()) {
            return new Metrics(successCount.get(), failureCount.get(), 0.0, 0.0);
        }
        
        snapshot.sort(Long::compareTo);
        var p99Index = (int) (snapshot.size() * 0.99);
        var p99LatencyMs = snapshot.get(p99Index) / 1_000_000.0;
        var throughput = successCount.get() / 10.0;
        
        return new Metrics(successCount.get(), failureCount.get(), p99LatencyMs, throughput);
    }

    public void close() {
        producer.close();
    }
}
