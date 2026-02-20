package com.uberlite.lesson27;

import org.apache.kafka.clients.admin.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicBootstrap {

    static final String DRIVER_TOPIC  = "driver-locations";
    static final String RIDER_TOPIC   = "rider-requests";
    static final String MATCH_TOPIC   = "match-events";
    static final int    PARTITIONS    = 12;
    static final short  REPLICATION   = 1;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        try (var admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic(DRIVER_TOPIC, PARTITIONS, REPLICATION),
                new NewTopic(RIDER_TOPIC,  PARTITIONS, REPLICATION),
                new NewTopic(MATCH_TOPIC,  PARTITIONS, REPLICATION)
            );

            try {
                admin.createTopics(topics).all().get();
                System.out.println("[BOOTSTRAP] Created topics with " + PARTITIONS + " partitions each.");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                    System.out.println("[BOOTSTRAP] Topics already exist, skipping.");
                } else {
                    throw e;
                }
            }

            // Validate co-partitioning
            var descriptions = admin.describeTopics(List.of(DRIVER_TOPIC, RIDER_TOPIC)).allTopicNames().get();
            descriptions.forEach((topic, desc) -> {
                int count = desc.partitions().size();
                System.out.printf("[BOOTSTRAP] %s → %d partitions%n", topic, count);
                if (count != PARTITIONS) {
                    throw new IllegalStateException("CO-PARTITION VIOLATION: " + topic + " has " + count + " partitions, expected " + PARTITIONS);
                }
            });
        }
    }
}
