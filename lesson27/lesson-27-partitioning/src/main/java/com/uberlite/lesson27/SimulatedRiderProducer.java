package com.uberlite.lesson27;

import com.uberlite.lesson27.Models.RiderRequest;
import com.uberlite.lesson27.Serialization.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import java.util.*;
import java.util.concurrent.atomic.LongAdder;

/**
 * Demo producer: sends RiderRequest events to rider-requests topic.
 * Key: H3 cell (Long) so they co-partition with driver-locations and trigger matches.
 * Runs for a fixed duration then exits (for demo.sh).
 */
public class SimulatedRiderProducer {

    static final String RIDER_TOPIC = TopicBootstrap.RIDER_TOPIC;
    static final int DURATION_SEC = 45;
    static final int REQUESTS_PER_SEC = 50;
    static final List<double[]> METRO_CENTERS = List.of(
        new double[]{41.83, -87.68}, new double[]{40.71, -74.00}, new double[]{34.05, -118.25},
        new double[]{29.76, -95.37}, new double[]{33.45, -112.07}, new double[]{25.77, -80.19},
        new double[]{39.74, -104.99}, new double[]{47.61, -122.33}
    );

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RiderRequestSerializer.class.getName());
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, H3ProducerPartitioner.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "lesson27-rider-demo");

        var sent = new LongAdder();
        var rng = new Random(12345);
        long deadline = System.currentTimeMillis() + DURATION_SEC * 1000L;
        long intervalMs = 1000 / Math.max(1, REQUESTS_PER_SEC);

        try (var producer = new KafkaProducer<Long, RiderRequest>(props)) {
            System.out.println("[RIDER DEMO] Sending rider requests for " + DURATION_SEC + "s (~" + REQUESTS_PER_SEC + "/sec)...");
            while (System.currentTimeMillis() < deadline) {
                double[] center = METRO_CENTERS.get(rng.nextInt(METRO_CENTERS.size()));
                double lat = center[0] + (rng.nextDouble() - 0.5) * 0.1;
                double lng = center[1] + (rng.nextDouble() - 0.5) * 0.1;
                String riderId = "rider-" + UUID.randomUUID().toString().substring(0, 8);
                RiderRequest req = RiderRequest.of(riderId, lat, lng);
                producer.send(new ProducerRecord<>(RIDER_TOPIC, req.h3Cell(), req), (m, e) -> {
                    if (e != null) System.err.println("[ERROR] " + e.getMessage());
                    else sent.increment();
                });
                Thread.sleep(intervalMs);
            }
        }
        System.out.println("[RIDER DEMO] Done. Sent " + sent.sum() + " rider requests. Dashboard should show non-zero match metrics.");
    }
}
