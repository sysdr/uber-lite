package com.uberlite.config;

import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
public class HealthController {

    private final Environment environment;

    public HealthController(Environment environment) {
        this.environment = environment;
    }

    private String read(String key) {
        if ("spring.kafka.bootstrap-servers".equals(key)) {
            String v = environment.getProperty("spring.kafka.bootstrapServers");
            if (v != null) return v;
        }
        return environment.getProperty(key);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("profile", environment.getActiveProfiles());
        response.put("kafkaBootstrap", read("spring.kafka.bootstrap-servers"));
        response.put("streamThreads", read("spring.kafka.streams.properties.num.stream.threads"));
        return response;
    }

    @GetMapping("/config")
    public Map<String, String> config() {
        Map<String, String> config = new HashMap<>();
        config.put("spring.kafka.bootstrap-servers", read("spring.kafka.bootstrap-servers"));
        config.put("spring.kafka.streams.application-id", read("spring.kafka.streams.application-id"));
        config.put("spring.kafka.streams.state-dir", read("spring.kafka.streams.state-dir"));
        config.put("spring.kafka.streams.properties.num.stream.threads", read("spring.kafka.streams.properties.num.stream.threads"));
        config.put("spring.kafka.streams.properties.linger.ms", read("spring.kafka.streams.properties.linger.ms"));
        return config;
    }
}
