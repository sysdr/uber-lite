package com.uberlite.firehose;

import com.uberlite.firehose.config.KafkaTopicAdmin;
import com.uberlite.firehose.metrics.MetricsHttpServer;
import com.uberlite.firehose.metrics.MetricsRegistry;
import com.uberlite.firehose.producer.FirehoseProducer;
import com.uberlite.firehose.topology.FirehoseTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Entry point for Lesson 25: Firehose Capstone.
 *
 * Startup sequence:
 *   1. Ensure Kafka topic exists (AdminClient)
 *   2. Start MetricsHttpServer on :8080
 *   3. Start KafkaStreams (4 StreamThreads, RocksDB state)
 *   4. Wait for RUNNING state
 *   5. Start FirehoseProducer (10k virtual driver threads)
 *   6. Block until shutdown signal
 */
public class FirehoseApplication {

    private static final Logger log = LoggerFactory.getLogger(FirehoseApplication.class);

    public static void main(String[] args) throws Exception {
        log.info("=== Lesson 25: Firehose Capstone ===");
        log.info("Target: 10k drivers | 3,333 msg/sec | 0 errors");

        // 1. Shared metrics registry
        var metrics = new MetricsRegistry();

        // 2. Ensure topic exists with correct partition count
        KafkaTopicAdmin.ensureTopics("localhost:9092");

        // 3. Metrics HTTP server
        var metricsServer = new MetricsHttpServer(metrics, 8080);
        metricsServer.start();

        // 4. Kafka Streams (consumer side)
        var topologyBuilder = new FirehoseTopology(metrics);
        var streams         = new KafkaStreams(topologyBuilder.build(), topologyBuilder.streamsConfig());
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Uncaught StreamThread exception", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();

        // 5. Wait for Streams to reach RUNNING state before starting producer
        log.info("Waiting for KafkaStreams to reach RUNNING state...");
        while (streams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(500);
        }
        log.info("KafkaStreams RUNNING. Starting producer fleet.");

        // 6. Producer (virtual thread driver fleet)
        var producer = new FirehoseProducer(metrics);
        producer.start();

        // 7. Shutdown hook
        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            log.info("Shutdown signal received");
            producer.shutdown();
            streams.close(java.time.Duration.ofSeconds(10));
            metricsServer.stop();
            latch.countDown();
        }));

        log.info("System running. Metrics: http://localhost:8080/metrics");
        log.info("Press Ctrl+C to stop.");
        latch.await();
        log.info("Firehose Capstone shutdown complete.");
    }
}
