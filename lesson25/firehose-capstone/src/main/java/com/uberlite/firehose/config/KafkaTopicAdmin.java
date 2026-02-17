package com.uberlite.firehose.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class KafkaTopicAdmin {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicAdmin.class);
    public static final String DRIVER_LOCATIONS_TOPIC = "driver-locations";
    public static final int    NUM_PARTITIONS         = 12;
    public static final short  REPLICATION_FACTOR     = 1; // Single broker for local dev

    public static void ensureTopics(String bootstrapServers) {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        try (var admin = AdminClient.create(props)) {
            Set<String> existing = admin.listTopics().names().get(15, TimeUnit.SECONDS);
            if (!existing.contains(DRIVER_LOCATIONS_TOPIC)) {
                var topic = new NewTopic(DRIVER_LOCATIONS_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR);
                // Tune topic-level retention for capstone: 30 minutes only
                topic.configs(Map.of(
                    "retention.ms", "1800000",
                    "segment.bytes", "104857600" // 100MB segments
                ));
                admin.createTopics(List.of(topic)).all().get(15, TimeUnit.SECONDS);
                log.info("Created topic '{}' with {} partitions", DRIVER_LOCATIONS_TOPIC, NUM_PARTITIONS);
            } else {
                log.info("Topic '{}' already exists", DRIVER_LOCATIONS_TOPIC);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Kafka topics", e);
        }
    }
}
