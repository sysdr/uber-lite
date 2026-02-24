package com.uberlite.lesson29;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public final class TopicAdmin {

    public static final String DRIVER_LOCATIONS_TOPIC = "driver-locations";
    public static final String RIDER_REQUESTS_TOPIC   = "rider-requests";
    public static final int    NUM_PARTITIONS          = 60;

    public static void ensureTopics(String bootstrapServers) throws Exception {
        var props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (var admin = AdminClient.create(props)) {
            var topics = List.of(
                new NewTopic(DRIVER_LOCATIONS_TOPIC, NUM_PARTITIONS, (short) 1),
                new NewTopic(RIDER_REQUESTS_TOPIC,   NUM_PARTITIONS, (short) 1)
            );
            try {
                admin.createTopics(topics).all().get();
                System.out.println("[TopicAdmin] Created topics with " + NUM_PARTITIONS + " partitions");
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("[TopicAdmin] Topics already exist — skipping creation");
                } else {
                    throw e;
                }
            }
        }
    }
}
