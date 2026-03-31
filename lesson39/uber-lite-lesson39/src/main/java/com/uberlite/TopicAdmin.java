package com.uberlite;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Startup guard: creates driver-updates and rider-requests with identical
 * partition counts, then validates co-partitioning parity before the
 * Streams topology is allowed to start.
 *
 * Run this BEFORE any producer or Streams application.
 */
public final class TopicAdmin {

    static final int TARGET_PARTITIONS = 60;
    static final short REPLICATION_FACTOR = 1; // single-broker dev cluster
    static final String DRIVER_TOPIC = "driver-updates";
    static final String RIDER_TOPIC  = "rider-requests";
    static final String BOOTSTRAP    = "localhost:9092";

    public static void main(String[] args) throws Exception {
        var props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP);
        props.put("request.timeout.ms", "15000");

        // Virtual Thread for blocking Admin API calls
        var future = Thread.ofVirtual().start(() -> {
            try (var admin = AdminClient.create(props)) {
                createTopics(admin);
                validateCoPartitioning(admin);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        future.join();
        System.out.println("[INFO] Topic admin complete.");
    }

    private static void createTopics(AdminClient admin)
            throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {

        var existing = admin.listTopics().names().get();
        var toCreate = List.of(DRIVER_TOPIC, RIDER_TOPIC).stream()
            .filter(t -> !existing.contains(t))
            .map(t -> new NewTopic(t, TARGET_PARTITIONS, REPLICATION_FACTOR)
                .configs(Map.of(
                    "retention.ms",          "600000",  // 10-min retention for dev
                    "min.insync.replicas",   "1",
                    "segment.bytes",         "104857600" // 100MB segments
                )))
            .toList();

        if (!toCreate.isEmpty()) {
            admin.createTopics(toCreate).all().get(15, TimeUnit.SECONDS);
            System.out.printf("[INFO] Created topics: %s%n",
                toCreate.stream().map(NewTopic::name).toList());
        } else {
            System.out.println("[INFO] Topics already exist, skipping creation.");
        }
    }

    /** Hard failure if partition counts diverge — prevents silent join corruption. */
    static void validateCoPartitioning(AdminClient admin)
            throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {

        var descriptions = admin.describeTopics(List.of(DRIVER_TOPIC, RIDER_TOPIC))
            .allTopicNames()
            .get(10, TimeUnit.SECONDS);

        var driverParts = descriptions.get(DRIVER_TOPIC).partitions().size();
        var riderParts  = descriptions.get(RIDER_TOPIC).partitions().size();

        System.out.printf("[INFO] %s: %d partitions%n", DRIVER_TOPIC, driverParts);
        System.out.printf("[INFO] %s:  %d partitions%n", RIDER_TOPIC,  riderParts);

        if (driverParts != riderParts) {
            throw new IllegalStateException(
                ("[FATAL] Co-partitioning violation detected! " +
                 "%s=%d, %s=%d. " +
                 "KStream joins will produce incorrect results. " +
                 "Fix: delete and recreate both topics with %d partitions.")
                    .formatted(DRIVER_TOPIC, driverParts,
                               RIDER_TOPIC,  riderParts,
                               TARGET_PARTITIONS));
        }
        if (driverParts != TARGET_PARTITIONS) {
            throw new IllegalStateException(
                "[FATAL] Expected %d partitions, found %d. Recreate topics."
                    .formatted(TARGET_PARTITIONS, driverParts));
        }
        System.out.println("[PASS] Co-partitioning contract verified: " +
                           driverParts + " partitions on both topics.");
    }
}
