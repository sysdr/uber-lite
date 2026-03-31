package com.uberlite;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.random.RandomGenerator;

/**
 * Produces simulated driver GPS pings at ~3,000 events/sec using Virtual Threads.
 *
 * RecordAccumulator tuning rationale:
 *   - linger.ms=20: batch records over 20ms window (~50 records/partition/sec × 0.02s = 1/batch avg)
 *   - batch.size=65536: 64KB batches reduce per-batch overhead in RecordAccumulator
 *   - compression.type=lz4: CPU-cheap, ~50% size reduction on GPS data
 */
public final class DriverProducer {

    // Mumbai bounding box (WGS84)
    private static final double LAT_MIN = 18.89, LAT_MAX = 19.27;
    private static final double LON_MIN = 72.77, LON_MAX = 73.05;

    // Bangalore bounding box
    private static final double BLAT_MIN = 12.83, BLAT_MAX = 13.14;
    private static final double BLON_MIN = 77.46, BLON_MAX = 77.78;

    private static final int EVENTS_PER_SEC = 3000;
    private static final int DURATION_SECS  = 30;

    public static void main(String[] args) throws InterruptedException {
        var props = producerProps();
        var sent  = new AtomicLong(0);
        var rng   = RandomGenerator.getDefault();

        System.out.println("[INFO] DriverProducer starting, target: " +
                           EVENTS_PER_SEC + " events/sec for " + DURATION_SECS + "s");

        // Virtual Thread executor — each send() is I/O-bound (network),
        // virtual threads prevent platform thread starvation.
        try (var exec   = Executors.newVirtualThreadPerTaskExecutor();
             var producer = new KafkaProducer<byte[], byte[]>(props)) {

            var start = System.currentTimeMillis();
            var deadline = start + (DURATION_SECS * 1000L);
            var intervalNs = 1_000_000_000L / EVENTS_PER_SEC; // ns between sends

            while (System.currentTimeMillis() < deadline) {
                var sendStart = System.nanoTime();

                exec.submit(() -> {
                    var driver = generateDriver(rng);
                    var record = new ProducerRecord<>(
                        TopicAdmin.DRIVER_TOPIC,
                        null,              // partition — computed by H3CityPartitioner
                        driver.timestampMs(),
                        driver.partitionKey(),
                        driver.toBytes()
                    );
                    producer.send(record, (meta, ex) -> {
                        if (ex != null) {
                            System.err.println("[ERROR] Send failed: " + ex.getMessage());
                        } else {
                            sent.incrementAndGet();
                        }
                    });
                    return null;
                });

                // Spin-wait to honour rate limit
                var elapsed = System.nanoTime() - sendStart;
                if (elapsed < intervalNs) {
                    Thread.sleep(0, (int)(intervalNs - elapsed));
                }
            }

            producer.flush();
        }

        System.out.printf("[INFO] DriverProducer complete. Total sent: %d%n", sent.get());
    }

    private static DriverLocationUpdate generateDriver(RandomGenerator rng) {
        // Alternate between Mumbai and Bangalore to test multi-city sharding
        boolean mumbai = rng.nextBoolean();
        double lat = mumbai
            ? LAT_MIN + rng.nextDouble() * (LAT_MAX - LAT_MIN)
            : BLAT_MIN + rng.nextDouble() * (BLAT_MAX - BLAT_MIN);
        double lon = mumbai
            ? LON_MIN + rng.nextDouble() * (LON_MAX - LON_MIN)
            : BLON_MIN + rng.nextDouble() * (BLON_MAX - BLON_MIN);

        return new DriverLocationUpdate(
            "drv-" + UUID.randomUUID().toString().substring(0, 8),
            lat, lon,
            System.currentTimeMillis(),
            "AVAILABLE"
        );
    }

    static Properties producerProps() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    TopicAdmin.BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        // Custom H3 geo-partitioner — THE critical config for co-partitioning
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                  "com.uberlite.H3CityPartitioner");
        // RecordAccumulator tuning for 3k events/sec across 60 partitions
        props.put(ProducerConfig.LINGER_MS_CONFIG,           "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,          "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4");
        props.put(ProducerConfig.ACKS_CONFIG,                "1");
        props.put(ProducerConfig.RETRIES_CONFIG,             "3");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,  "false"); // dev only
        return props;
    }
}
