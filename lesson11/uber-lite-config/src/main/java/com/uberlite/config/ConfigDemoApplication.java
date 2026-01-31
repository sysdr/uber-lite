package com.uberlite.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;

import java.util.Arrays;

@SpringBootApplication
public class ConfigDemoApplication {

    private static final Logger log = LoggerFactory.getLogger(ConfigDemoApplication.class);

    private final Environment env;

    public ConfigDemoApplication(Environment env) {
        this.env = env;
    }

    public static void main(String[] args) {
        SpringApplication.run(ConfigDemoApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onStartup() {
        String[] activeProfiles = env.getActiveProfiles();
        String bootstrap = env.getProperty("spring.kafka.bootstrapServers");
        if (bootstrap == null) bootstrap = env.getProperty("spring.kafka.bootstrap-servers");
        if (activeProfiles.length > 0 && bootstrap != null && bootstrap.contains("localhost")) {
            log.error("Profile is {} but spring.kafka.bootstrap-servers contains localhost: {}", Arrays.toString(activeProfiles), bootstrap);
            throw new IllegalStateException("When profile is not default, spring.kafka.bootstrap-servers must not contain localhost; got: " + bootstrap);
        }

        System.out.println("\n" + "=".repeat(80));
        System.out.println("UBER-LITE CONFIGURATION DEMO - LESSON 11");
        System.out.println("=".repeat(80));

        System.out.println("Active Profiles: " + Arrays.toString(activeProfiles));
        
        System.out.println("\n--- Resolved Configuration ---");
        printConfig("Kafka Bootstrap Servers", "spring.kafka.bootstrap-servers");
        printConfig("Application ID", "spring.kafka.streams.application-id");
        printConfig("State Store Directory", "spring.kafka.streams.state-dir");
        printConfig("Stream Threads", "spring.kafka.streams.properties.num.stream.threads");
        printConfig("Replication Factor", "spring.kafka.streams.replication-factor");
        printConfig("Linger MS", "spring.kafka.streams.properties.linger.ms");
        printConfig("Batch Size", "spring.kafka.streams.properties.batch.size");
        printConfig("Commit Interval MS", "spring.kafka.streams.properties.commit.interval.ms");
        
        System.out.println("\n--- Network Validation ---");
        validateKafkaConnection();
        
        System.out.println("=".repeat(80) + "\n");
    }
    
    private void printConfig(String label, String key) {
        String value = env.getProperty(key, "NOT SET");
        System.out.printf("%-30s: %s%n", label, value);
    }
    
    private void validateKafkaConnection() {
        String bootstrapServers = env.getProperty("spring.kafka.bootstrap-servers");
        System.out.println("Attempting connection to: " + bootstrapServers);
        
        // Parse broker addresses
        if (bootstrapServers != null) {
            String[] brokers = bootstrapServers.split(",");
            System.out.println("Discovered " + brokers.length + " broker(s):");
            for (String broker : brokers) {
                System.out.println("  - " + broker);
            }
        }
    }
}
