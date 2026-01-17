package com.uberlite.lesson01;

import com.uberlite.lesson01.kafka.KafkaLocationProducer;
import com.uberlite.lesson01.kafka.LocationStateProcessor;
import com.uberlite.lesson01.loadgen.LoadGenerator;
import com.uberlite.lesson01.postgres.PostgresLocationUpdater;
import com.uberlite.lesson01.rest.StateQueryServer;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static PostgresLocationUpdater pgUpdater;
    private static KafkaLocationProducer kafkaProducer;
    private static StateQueryServer queryServer;
    
    public static void main(String[] args) throws Exception {
        var mode = args.length > 0 ? args[0] : "both";
        
        System.out.println("🚀 Uber-Lite Lesson 01: Monolith vs Event Log");
        System.out.println("Mode: " + mode);
        System.out.println("============================================\n");
        
        var executor = Executors.newScheduledThreadPool(2);
        
        // Start REST API server (dashboard is always available)
        LocationStateProcessor stateProcessor = null;
        if ("kafka".equals(mode) || "both".equals(mode)) {
            stateProcessor = new LocationStateProcessor("localhost:9092");
            stateProcessor.start();
        }
        
        queryServer = stateProcessor != null 
            ? new StateQueryServer(8080, stateProcessor)
            : new StateQueryServer(8080);
        queryServer.start();
        
        if ("postgres".equals(mode) || "both".equals(mode)) {
            runPostgresTest(executor);
        }
        
        if ("kafka".equals(mode) || "both".equals(mode)) {
            runKafkaTest(executor);
        }
        
        // Keep running
        Thread.sleep(Long.MAX_VALUE);
    }
    
    private static void runPostgresTest(ScheduledExecutorService executor) {
        System.out.println("📊 Starting PostgreSQL Test...");
        pgUpdater = new PostgresLocationUpdater("jdbc:postgresql://localhost:5432/postgres");
        var loadGen = new LoadGenerator(1000, 3000);
        
        loadGen.start(pgUpdater::updateLocation);
        
        // Set metrics supplier for dashboard
        if (queryServer != null) {
            queryServer.setPostgresMetricsSupplier(pgUpdater::getMetrics);
        }
        
        executor.scheduleAtFixedRate(() -> {
            var metrics = pgUpdater.getMetrics();
            System.out.printf("[PostgreSQL] Throughput: %.0f/s | p99: %.2fms | Success: %d | Failures: %d%n",
                metrics.throughput(), metrics.p99LatencyMs(), 
                metrics.successCount(), metrics.failureCount());
        }, 10, 10, TimeUnit.SECONDS);
    }
    
    private static void runKafkaTest(ScheduledExecutorService executor) throws Exception {
        System.out.println("📊 Starting Kafka Test...");
        kafkaProducer = new KafkaLocationProducer("localhost:9092");
        
        var loadGen = new LoadGenerator(1000, 3000);
        loadGen.start(kafkaProducer::sendLocation);
        
        // Set metrics supplier for dashboard
        if (queryServer != null) {
            queryServer.setKafkaMetricsSupplier(kafkaProducer::getMetrics);
        }
        
        executor.scheduleAtFixedRate(() -> {
            var metrics = kafkaProducer.getMetrics();
            System.out.printf("[Kafka] Throughput: %.0f/s | p99: %.2fms | Success: %d | Failures: %d%n",
                metrics.throughput(), metrics.p99LatencyMs(), 
                metrics.successCount(), metrics.failureCount());
        }, 10, 10, TimeUnit.SECONDS);
    }
}
