package com.uberlite.async;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class ProducerMetrics {
    private final LongAdder successCount = new LongAdder();
    private final LongAdder failureCount = new LongAdder();
    private final AtomicLong lastSuccessTimestamp = new AtomicLong();
    private final AtomicLong totalLatency = new AtomicLong();
    
    public void recordSuccess(long sendTimestamp) {
        successCount.increment();
        lastSuccessTimestamp.set(System.currentTimeMillis());
        long latency = System.currentTimeMillis() - sendTimestamp;
        totalLatency.addAndGet(latency);
    }
    
    public void recordFailure() {
        failureCount.increment();
    }
    
    public long getSuccessCount() {
        return successCount.sum();
    }
    
    public long getFailureCount() {
        return failureCount.sum();
    }
    
    public double getAverageLatency() {
        long successes = successCount.sum();
        return successes == 0 ? 0 : (double) totalLatency.get() / successes;
    }
    
    public void printStats() {
        long total = successCount.sum() + failureCount.sum();
        System.out.printf("""
            
            ═══════════════════════════════════════════════════
            📊 PRODUCER METRICS
            ═══════════════════════════════════════════════════
            Total Sent:       %,d
            Successes:        %,d (%.2f%%)
            Failures:         %,d (%.2f%%)
            Avg Latency:      %.2f ms
            ═══════════════════════════════════════════════════
            
            """,
            total,
            successCount.sum(),
            (successCount.sum() * 100.0 / Math.max(1, total)),
            failureCount.sum(),
            (failureCount.sum() * 100.0 / Math.max(1, total)),
            getAverageLatency()
        );
    }
}
