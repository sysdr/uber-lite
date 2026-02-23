package com.uberlite;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicAdmin {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "driver-locations";
    private static final int PARTITIONS = 64;
    private static final short REPLICATION = 1;

    public static void ensureTopic() throws ExecutionException, InterruptedException {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

        try (var admin = AdminClient.create(props)) {
            var existing = admin.listTopics().names().get();
            if (existing.contains(TOPIC)) {
                System.out.printf("[TopicAdmin] Topic '%s' already exists%n", TOPIC);
                return;
            }

            var newTopic = new NewTopic(TOPIC, PARTITIONS, REPLICATION)
                .configs(Map.of(
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "lz4",
                    TopicConfig.RETENTION_MS_CONFIG, "600000"
                ));

            admin.createTopics(List.of(newTopic)).all().get();
            System.out.printf("[TopicAdmin] Created '%s' with %d partitions%n", TOPIC, PARTITIONS);
        }
    }
}
