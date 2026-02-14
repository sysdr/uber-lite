package com.uberlite.stress;

import java.util.concurrent.*;

/**
 * Lesson 24: Stress Testing Entry Point
 * Runs for TEST_DURATION_MINUTES minutes at TARGET_RATE_PER_SEC events/sec.
 * Exits with code 0 on pass, 1 on failure.
 */
public class StressTestMain {

    static final int    TARGET_RATE_PER_SEC    = 5_000;
    static final int    TEST_DURATION_MINUTES  = 60;
    static final String BOOTSTRAP_SERVERS      = "localhost:9092,localhost:9093,localhost:9094";
    static final String INPUT_TOPIC            = "driver-locations";
    static final String OUTPUT_TOPIC           = "driver-locations-processed";
    static final int    METRICS_PORT           = 9090;

    public static void main(String[] args) throws Exception {
        System.out.println("=== Lesson 24: Stress Test Starting ===");
        System.out.printf("Target: %,d events/sec for %d minutes%n",
                TARGET_RATE_PER_SEC, TEST_DURATION_MINUTES);

        // Start metrics HTTP server
        var metricsServer = new MetricsHttpServer(METRICS_PORT);
        metricsServer.start();

        // Start Kafka Streams processor
        var streamsApp = new DriverLocationStreamsApp(
                BOOTSTRAP_SERVERS, INPUT_TOPIC, OUTPUT_TOPIC);
        streamsApp.start();

        // Allow streams to initialize
        Thread.sleep(5_000);

        // Run stress test orchestrator
        var orchestrator = new StressTestOrchestrator(
                BOOTSTRAP_SERVERS, INPUT_TOPIC,
                TARGET_RATE_PER_SEC, TEST_DURATION_MINUTES,
                metricsServer);

        System.out.println("==> Starting load generation...");
        boolean passed = orchestrator.run();

        // Cleanup
        System.out.println("==> Draining...");
        Thread.sleep(5_000);
        streamsApp.stop();
        metricsServer.stop();

        System.out.printf("%n=== STRESS TEST %s ===%n", passed ? "PASSED" : "FAILED");
        orchestrator.printFinalReport();
        System.exit(passed ? 0 : 1);
    }
}
