package com.uberlite.backpressure;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.Metric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class BackpressureProducer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BackpressureProducer.class);
    
    private final KafkaProducer<String, byte[]> producer;
    private final boolean backpressureEnabled;
    private final AtomicLong backoffMillis = new AtomicLong(0);
    private final LongAdder bufferExhaustionCount = new LongAdder();
    private final LongAdder successCount = new LongAdder();
    private static final long MAX_BACKOFF_MS = 5000;
    private static final long MIN_BUFFER_BYTES = 4_194_304; // 4MB threshold

    public BackpressureProducer(Properties props, boolean backpressureEnabled) {
        this.producer = new KafkaProducer<>(props);
        this.backpressureEnabled = backpressureEnabled;
    }

    public void sendWithBackpressure(String topic, DriverLocation location) throws InterruptedException {
        if (!backpressureEnabled) {
            // Naive mode - will crash under load
            producer.send(new ProducerRecord<>(topic, location.driverId(), location.toBytes()));
            return;
        }

        while (true) {
            try {
                // Check buffer availability BEFORE attempting send
                double availableBytes = getBufferAvailableBytes();
                if (availableBytes < MIN_BUFFER_BYTES) {
                    LOG.warn("Buffer pressure detected: {}MB available, throttling...", 
                        availableBytes / 1_048_576);
                    Thread.sleep(50); // Brief pause to let buffer drain
                    continue;
                }

                producer.send(new ProducerRecord<>(topic, location.driverId(), location.toBytes()));
                successCount.increment();
                backoffMillis.set(0); // Success - reset backoff
                return;

            } catch (KafkaException e) {
                if (e.getMessage() != null && e.getMessage().contains("buffer memory")) {
                    bufferExhaustionCount.increment();
                    
                    // Exponential backoff with jitter
                    long backoff = backoffMillis.updateAndGet(
                        curr -> Math.min(curr == 0 ? 100 : curr * 2, MAX_BACKOFF_MS)
                    );
                    long jitter = ThreadLocalRandom.current().nextLong(backoff / 2, backoff);
                    
                    LOG.warn("Buffer exhausted (count: {}), backing off {}ms", 
                        bufferExhaustionCount.sum(), jitter);
                    Thread.sleep(jitter);
                } else {
                    throw e;
                }
            }
        }
    }

    private double getBufferAvailableBytes() {
        return producer.metrics().values().stream()
            .filter(m -> "buffer-available-bytes".equals(m.metricName().name()))
            .findFirst()
            .map(Metric::metricValue)
            .map(v -> ((Number) v).doubleValue())
            .orElse(16_777_216.0); // Default to 16MB if metric not found
    }

    public long getBufferExhaustionCount() {
        return bufferExhaustionCount.sum();
    }

    public long getSuccessCount() {
        return successCount.sum();
    }

    public double getBufferUtilization() {
        double available = getBufferAvailableBytes();
        return (16_777_216.0 - available) / 16_777_216.0 * 100.0;
    }

    @Override
    public void close() {
        producer.close();
    }
}
