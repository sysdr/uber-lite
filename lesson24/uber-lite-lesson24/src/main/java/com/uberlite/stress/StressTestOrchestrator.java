package com.uberlite.stress;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Lesson 24: Stress Test Orchestrator
 *
 * Runs N_PARTITIONS virtual threads, each targeting RATE/N_PARTITIONS events/sec.
 * Uses token-bucket rate limiting within each thread for accurate throughput control.
 * Monitors heap usage, GC overhead, and producer send rate every 10 seconds.
 */
public class StressTestOrchestrator {

    private static final int    N_PARTITIONS       = 6;
    private static final int    BURST_CAPACITY      = 1_000;
    private static final double HEAP_FAIL_THRESHOLD = 0.85;  // 85% heap → fail
    private static final double RATE_FAIL_THRESHOLD = 0.92;  // <92% target rate → fail
    private static final int    CONSECUTIVE_FAIL_LIMIT = 3;

    private final String bootstrapServers;
    private final String inputTopic;
    private final int    targetRatePerSec;
    private final long   testDurationMs;
    private final MetricsHttpServer metricsServer;

    // Shared counters (lock-free)
    private final AtomicLong totalSent       = new AtomicLong(0);
    private final AtomicLong totalErrors     = new AtomicLong(0);
    private final AtomicLong windowSent      = new AtomicLong(0);
    private final AtomicLong latencyTotalNs  = new AtomicLong(0);

    // Failure tracking
    private final List<String> failureReasons = new CopyOnWriteArrayList<>();
    private volatile boolean aborted = false;

    public StressTestOrchestrator(String bootstrapServers, String inputTopic,
                                   int targetRatePerSec, int durationMinutes,
                                   MetricsHttpServer metricsServer) {
        this.bootstrapServers  = bootstrapServers;
        this.inputTopic        = inputTopic;
        this.targetRatePerSec  = targetRatePerSec;
        this.testDurationMs    = (long) durationMinutes * 60 * 1000;
        this.metricsServer     = metricsServer;
    }

    /**
     * @return true if test passed all stability criteria
     */
    public boolean run() throws Exception {
        var spatialGen = new SpatialDataGenerator();
        var producer   = buildProducer();
        var latch      = new CountDownLatch(1);

        // Virtual thread per partition
        int ratePerPartition = targetRatePerSec / N_PARTITIONS;
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        long endTime = System.currentTimeMillis() + testDurationMs;

        System.out.printf("==> Spawning %d virtual-thread producers @ %,d events/sec each%n",
                N_PARTITIONS, ratePerPartition);

        List<Future<?>> futures = new ArrayList<>();
        for (int p = 0; p < N_PARTITIONS; p++) {
            final int partition = p;
            futures.add(executor.submit(() -> {
                producerLoop(producer, spatialGen, partition, ratePerPartition, endTime);
                return null;
            }));
        }

        // Monitor thread (OS thread — intentional, this is coordination not I/O)
        var monitorFuture = executor.submit(() -> {
            monitorLoop(endTime);
            latch.countDown();
            return null;
        });

        // Wait for all producers to finish
        for (var f : futures) {
            try { f.get(); } catch (ExecutionException e) {
                failureReasons.add("Producer error: " + e.getCause().getMessage());
            }
        }
        monitorFuture.cancel(true);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        producer.flush();
        producer.close(Duration.ofSeconds(10));

        return failureReasons.isEmpty() && !aborted;
    }

    private void producerLoop(KafkaProducer<String, byte[]> producer,
                               SpatialDataGenerator gen,
                               int partition, int ratePerSec, long endTime) {
        // Token bucket state — per-thread, no contention
        long tokens      = BURST_CAPACITY;
        long lastRefillNs = System.nanoTime();
        long tokenCapacity = BURST_CAPACITY;

        while (System.currentTimeMillis() < endTime && !aborted) {
            // Refill tokens
            long nowNs = System.nanoTime();
            long elapsedNs = nowNs - lastRefillNs;
            long newTokens = (elapsedNs * ratePerSec) / 1_000_000_000L;
            if (newTokens > 0) {
                tokens = Math.min(tokens + newTokens, tokenCapacity);
                lastRefillNs = nowNs;
            }

            if (tokens <= 0) {
                Thread.onSpinWait(); // x86: PAUSE instruction, reduces pipeline pressure
                continue;
            }
            tokens--;

            var event    = gen.nextEvent();
            var record   = new ProducerRecord<>(
                    inputTopic, partition, event.h3Cell(), event.toJsonBytes());
            long sendNs  = System.nanoTime();

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    totalErrors.incrementAndGet();
                } else {
                    long latNs = System.nanoTime() - sendNs;
                    totalSent.incrementAndGet();
                    windowSent.incrementAndGet();
                    latencyTotalNs.addAndGet(latNs);
                }
            });
        }
    }

    private void monitorLoop(long endTime) {
        var runtime = Runtime.getRuntime();
        int consecutiveRateFails = 0;
        long monitorIntervalMs   = 10_000; // sample every 10s
        long lastWindowSent      = 0;

        while (System.currentTimeMillis() < endTime && !aborted) {
            try { Thread.sleep(monitorIntervalMs); } catch (InterruptedException e) { break; }

            // Throughput
            long currentWindowSent = windowSent.getAndSet(0);
            double actualRate = (double) currentWindowSent / (monitorIntervalMs / 1000.0);

            // Heap
            long usedHeap  = runtime.totalMemory() - runtime.freeMemory();
            long maxHeap   = runtime.maxMemory();
            double heapPct = (double) usedHeap / maxHeap;

            // Average latency this window
            long totalLat  = latencyTotalNs.getAndSet(0);
            double avgLatMs = currentWindowSent > 0
                    ? (double) totalLat / currentWindowSent / 1_000_000.0
                    : 0;

            long elapsed = testDurationMs - (endTime - System.currentTimeMillis());
            System.out.printf("[%5.1fs] Rate: %,6.0f/sec | Heap: %4.1f%% | AvgLat: %5.2fms | " +
                            "Total: %,d | Errors: %d%n",
                    elapsed / 1000.0, actualRate, heapPct * 100, avgLatMs,
                    totalSent.get(), totalErrors.get());

            // Update metrics server
            metricsServer.update(actualRate, heapPct, avgLatMs,
                    totalSent.get(), totalErrors.get());

            // Rate stability check (skip first sample — warmup)
            if (lastWindowSent > 0) {
                double rateFraction = actualRate / targetRatePerSec;
                if (rateFraction < RATE_FAIL_THRESHOLD) {
                    consecutiveRateFails++;
                    System.err.printf("WARN: Rate %.1f%% of target (%d/%d consecutive)%n",
                            rateFraction * 100, consecutiveRateFails, CONSECUTIVE_FAIL_LIMIT);
                    if (consecutiveRateFails >= CONSECUTIVE_FAIL_LIMIT) {
                        failureReasons.add(String.format(
                                "Rate failure: %.0f/sec < %.0f/sec threshold for %d consecutive samples",
                                actualRate, targetRatePerSec * RATE_FAIL_THRESHOLD,
                                CONSECUTIVE_FAIL_LIMIT));
                        aborted = true;
                    }
                } else {
                    consecutiveRateFails = 0;
                }
            }
            lastWindowSent = currentWindowSent;

            // Heap pressure check
            if (heapPct > HEAP_FAIL_THRESHOLD) {
                failureReasons.add(String.format(
                        "Heap pressure: %.1f%% > %.1f%% threshold",
                        heapPct * 100, HEAP_FAIL_THRESHOLD * 100));
                aborted = true;
            }
        }
    }

    private KafkaProducer<String, byte[]> buildProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,     bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        // RecordAccumulator tuning — the core of batching performance
        props.put(ProducerConfig.LINGER_MS_CONFIG,              "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,             "65536");  // 64KB
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,       "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,          "67108864"); // 64MB buffer
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG,                   "1");       // leader ack
        props.put(ProducerConfig.RETRIES_CONFIG,                "3");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        // Timeouts
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,     "15000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,    "30000");
        // JMX metrics client ID
        props.put(ProducerConfig.CLIENT_ID_CONFIG,              "stress-test-producer");
        return new KafkaProducer<>(props);
    }

    public void printFinalReport() {
        System.out.printf("%nFinal Report:%n");
        System.out.printf("  Total events sent : %,d%n", totalSent.get());
        System.out.printf("  Total errors      : %,d%n", totalErrors.get());
        System.out.printf("  Duration          : %d minutes%n",
                (int)(testDurationMs / 60_000));
        if (!failureReasons.isEmpty()) {
            System.out.println("  Failure reasons:");
            failureReasons.forEach(r -> System.out.println("    - " + r));
        }
    }
}
