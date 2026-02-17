package com.uberlite.firehose.producer;

import com.uber.h3core.H3Core;
import com.uberlite.firehose.config.KafkaTopicAdmin;
import com.uberlite.firehose.metrics.MetricsRegistry;
import com.uberlite.firehose.model.DriverLocationEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates 10,000 concurrent drivers using Java 21 virtual threads.
 *
 * Physics:
 *   - Single KafkaProducer instance (thread-safe) shared across all VTs
 *   - 10k VTs × 3s sleep interval = 3,333 events/sec aggregate
 *   - H3 cell at resolution 9 used as record key → spatial partition affinity
 *   - Async callbacks: producer never blocks on broker acknowledgment
 *   - LZ4 compression + 128KB batches → RecordAccumulator amortizes network cost
 */
public class FirehoseProducer {

    private static final Logger log = LoggerFactory.getLogger(FirehoseProducer.class);

    // NYC bounding box
    private static final double LAT_MIN =  40.4774, LAT_MAX = 40.9176;
    private static final double LON_MIN = -74.2591, LON_MAX = -73.7004;

    private static final int    DRIVER_COUNT        = 10_000;
    private static final long   SEND_INTERVAL_MS    = 3_000; // ~3333 msg/sec across fleet
    private static final int    JITTER_MS           = 400;

    private final MetricsRegistry metrics;
    private final AtomicBoolean   shutdown  = new AtomicBoolean(false);
    private final AtomicLong      windowSent = new AtomicLong(0);

    private KafkaProducer<String, byte[]> producer;
    private ExecutorService              executor;
    private ScheduledExecutorService     throughputReporter;
    private H3Core                       h3;

    public FirehoseProducer(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    public void start() throws IOException {
        h3 = H3Core.newInstance(); // JNI bridge: initialize ONCE, reuse across all VTs

        producer  = new KafkaProducer<>(producerConfig());
        // Virtual-thread-per-task executor: 10k tasks share ~CPU-count carrier threads
        executor  = Executors.newVirtualThreadPerTaskExecutor();

        for (int i = 0; i < DRIVER_COUNT; i++) {
            final int driverId = i;
            executor.submit(() -> runDriver(driverId));
        }

        // Throughput reporter: samples window count every second
        throughputReporter = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("throughput-reporter").factory());
        throughputReporter.scheduleAtFixedRate(() -> {
            long rate = windowSent.getAndSet(0);
            metrics.setProduceThroughput(rate);
            log.info("[PRODUCER] {}/sec | total={} | errors={}",
                     rate, metrics.getTotalProduced(), metrics.getTotalErrors());
        }, 1, 1, TimeUnit.SECONDS);

        log.info("FirehoseProducer started: {} virtual driver threads", DRIVER_COUNT);
    }

    private void runDriver(int driverId) {
        var rng    = ThreadLocalRandom.current();
        double lat = LAT_MIN + rng.nextDouble() * (LAT_MAX - LAT_MIN);
        double lon = LON_MIN + rng.nextDouble() * (LON_MAX - LON_MIN);

        while (!shutdown.get()) {
            try {
                // Random walk: driver moves ~100m per tick (H3 res-9 cell = ~0.1 km²)
                lat = clamp(lat + (rng.nextDouble() - 0.5) * 0.001, LAT_MIN, LAT_MAX);
                lon = clamp(lon + (rng.nextDouble() - 0.5) * 0.001, LON_MIN, LON_MAX);

                var event  = new DriverLocationEvent(driverId, lat, lon, System.currentTimeMillis());
                var h3Cell = h3.latLngToCell(lat, lon, 9); // Resolution 9 ≈ 0.1 km²
                var key    = Long.toString(h3Cell);         // H3 cell as partition key
                var value  = DriverLocationEvent.serialize(event);

                long sendTs = System.currentTimeMillis();
                producer.send(
                    new ProducerRecord<>(KafkaTopicAdmin.DRIVER_LOCATIONS_TOPIC, key, value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            metrics.incrementErrors();
                        } else {
                            metrics.recordProduced();
                            windowSent.incrementAndGet();
                            metrics.recordProduceLatency(System.currentTimeMillis() - sendTs);
                        }
                    }
                );

                // Virtual thread yields to carrier during sleep — zero OS blocking
                Thread.sleep(SEND_INTERVAL_MS - JITTER_MS + rng.nextLong(JITTER_MS * 2));

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public void shutdown() {
        shutdown.set(true);
        if (throughputReporter != null) throughputReporter.shutdownNow();
        if (executor != null)          executor.shutdownNow();
        if (producer != null)          producer.close(java.time.Duration.ofSeconds(5));
        log.info("FirehoseProducer shut down");
    }

    private Properties producerConfig() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,              "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,           StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,         ByteArraySerializer.class.getName());
        // RecordAccumulator tuning: let batches fill before flushing
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,                     131_072);   // 128KB
        props.put(ProducerConfig.LINGER_MS_CONFIG,                      5);         // 5ms fill window
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,               "lz4");     // Best ratio for GPS data
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,                  134_217_728L); // 128MB accumulator
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ACKS_CONFIG,                           "1");        // Leader ack only
        props.put(ProducerConfig.RETRIES_CONFIG,                        3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,               100);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,             30_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,            60_000);
        // Metadata prefetch for all partitions
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG,               60_000);
        return props;
    }

    private static double clamp(double val, double min, double max) {
        return Math.max(min, Math.min(max, val));
    }
}
