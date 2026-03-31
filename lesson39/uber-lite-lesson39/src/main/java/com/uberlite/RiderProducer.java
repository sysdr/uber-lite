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
 * Produces simulated rider pickup requests.
 * Uses identical H3CityPartitioner — guarantees rider records
 * in the same H3 R4 cell as driver records land on the same partition.
 */
public final class RiderProducer {

    private static final double LAT_MIN = 18.89, LAT_MAX = 19.27;
    private static final double LON_MIN = 72.77, LON_MAX = 73.05;
    private static final double BLAT_MIN = 12.83, BLAT_MAX = 13.14;
    private static final double BLON_MIN = 77.46, BLON_MAX = 77.78;

    private static final int EVENTS_PER_SEC = 1000;
    private static final int DURATION_SECS  = 30;

    public static void main(String[] args) throws InterruptedException {
        var props = riderProps();
        var sent  = new AtomicLong(0);
        var rng   = RandomGenerator.getDefault();

        System.out.println("[INFO] RiderProducer starting...");

        try (var exec     = Executors.newVirtualThreadPerTaskExecutor();
             var producer = new KafkaProducer<byte[], byte[]>(props)) {

            var deadline   = System.currentTimeMillis() + (DURATION_SECS * 1000L);
            var intervalNs = 1_000_000_000L / EVENTS_PER_SEC;

            while (System.currentTimeMillis() < deadline) {
                var sendStart = System.nanoTime();

                exec.submit(() -> {
                    boolean mumbai = rng.nextBoolean();
                    double lat = mumbai
                        ? LAT_MIN + rng.nextDouble() * (LAT_MAX - LAT_MIN)
                        : BLAT_MIN + rng.nextDouble() * (BLAT_MAX - BLAT_MIN);
                    double lon = mumbai
                        ? LON_MIN + rng.nextDouble() * (LON_MAX - LON_MIN)
                        : BLON_MIN + rng.nextDouble() * (BLON_MAX - BLON_MIN);

                    var rider  = new RiderRequest(
                        "rdr-" + UUID.randomUUID().toString().substring(0, 8),
                        lat, lon, System.currentTimeMillis()
                    );
                    var record = new ProducerRecord<>(
                        TopicAdmin.RIDER_TOPIC,
                        null,
                        rider.timestampMs(),
                        rider.partitionKey(),
                        rider.toBytes()
                    );
                    producer.send(record, (meta, ex) -> {
                        if (ex != null) {
                            System.err.println("[ERROR] " + ex.getMessage());
                        } else {
                            sent.incrementAndGet();
                        }
                    });
                    return null;
                });

                var elapsed = System.nanoTime() - sendStart;
                if (elapsed < intervalNs) {
                    Thread.sleep(0, (int)(intervalNs - elapsed));
                }
            }
            producer.flush();
        }

        System.out.printf("[INFO] RiderProducer complete. Sent: %d%n", sent.get());
    }

    static Properties riderProps() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,    TopicAdmin.BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        // Same partitioner as DriverProducer — this is the co-partitioning contract
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
                  "com.uberlite.H3CityPartitioner");
        props.put(ProducerConfig.LINGER_MS_CONFIG,           "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,          "65536");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,    "lz4");
        props.put(ProducerConfig.ACKS_CONFIG,                "1");
        return props;
    }
}
