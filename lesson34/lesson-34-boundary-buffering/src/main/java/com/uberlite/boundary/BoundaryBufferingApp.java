package com.uberlite.boundary;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Main entry point for Lesson 34: Boundary Buffering.
 *
 * Starts:
 *   1. MetricsServer on :7072
 *   2. KafkaStreams with BoundaryBufferingTopology
 *   3. LoadGenerator (virtual threads, 500 interior + 100 boundary drivers)
 */
public final class BoundaryBufferingApp {

    private static final Logger log = LoggerFactory.getLogger(BoundaryBufferingApp.class);

    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");

    public static void main(String[] args) throws Exception {
        var metrics  = new BoundaryMetrics();
        var detector = new BoundaryDetector();

        // --- Metrics HTTP server ---
        var metricsServer = new MetricsServer(metrics);
        metricsServer.start();

        // --- Build topology ---
        Topology topology = BoundaryBufferingTopology.build(detector, metrics);
        log.info("Topology:\n{}", topology.describe());

        // --- Streams config ---
        var streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "lesson34-boundary-buffering");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
        streamsProps.put(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG,
                BoundaryRocksDBConfig.class.getName());
        // Consumer group for lag monitoring
        streamsProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var streams = new KafkaStreams(topology, streamsProps);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            log.info("Shutdown hook: closing streams...");
            streams.close(Duration.ofSeconds(30));
            metricsServer.stop();
        }));

        streams.setUncaughtExceptionHandler((ex) -> {
            log.error("Uncaught streams exception", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        log.info("Kafka Streams started. State: {}", streams.state());

        // Wait for RUNNING state before generating load
        while (streams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(200);
        }
        log.info("Streams reached RUNNING state. Starting load generator.");

        // --- Load generator (virtual threads: 500 interior + 100 boundary drivers) ---
        var loadGen = new LoadGenerator(BOOTSTRAP_SERVERS, 500, 100);
        Thread.ofVirtual().start(() -> {
            try {
                loadGen.run();
                log.info("Load generation finished. Total events sent: {}", loadGen.getTotalSent());
            } catch (Exception e) {
                log.error("Load generator failed", e);
            }
        });

        // Keep alive — metrics server serves until SIGTERM
        Thread.currentThread().join();
    }
}
