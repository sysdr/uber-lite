package com.uberlite.firehose.metrics;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Lock-free metrics registry using LongAdder for high-contention counters.
 * Latency percentiles computed from a fixed-size circular buffer — no external dependencies.
 */
public class MetricsRegistry {

    private final LongAdder totalProduced  = new LongAdder();
    private final LongAdder totalConsumed  = new LongAdder();
    private final LongAdder totalErrors    = new LongAdder();

    // Throughput snapshot updated by Punctuator every second
    private volatile long currentProduceThroughput  = 0;
    private volatile long currentConsumeThroughput  = 0;

    // Rolling window of produce latencies (ms) — last 10,000 observations
    private final ArrayBlockingQueue<Long> latencyWindow = new ArrayBlockingQueue<>(10_000);

    public void recordProduced()          { totalProduced.increment(); }
    public void recordConsumed()          { totalConsumed.increment(); }
    public void incrementErrors()         { totalErrors.increment(); }

    public void recordProduceLatency(long ms) {
        // Offer, not put — never block the producer callback thread
        if (!latencyWindow.offer(ms)) {
            latencyWindow.poll(); // evict oldest
            latencyWindow.offer(ms);
        }
    }

    public void setProduceThroughput(long rate)  { currentProduceThroughput = rate; }
    public void setConsumeThroughput(long rate)  { currentConsumeThroughput = rate; }

    public long getProduceThroughput()  { return currentProduceThroughput; }
    public long getConsumeThroughput()  { return currentConsumeThroughput; }
    public long getTotalProduced()      { return totalProduced.sum(); }
    public long getTotalConsumed()      { return totalConsumed.sum(); }
    public long getTotalErrors()        { return totalErrors.sum(); }

    public long getP99LatencyMs() {
        var samples = latencyWindow.stream().mapToLong(Long::longValue).sorted().toArray();
        if (samples.length == 0) return 0L;
        int idx = (int) Math.ceil(samples.length * 0.99) - 1;
        return samples[Math.max(0, idx)];
    }

    public String toJson() {
        return String.format("""
                {
                  "produce_throughput_per_sec": %d,
                  "consume_throughput_per_sec": %d,
                  "total_produced": %d,
                  "total_consumed": %d,
                  "total_errors": %d,
                  "p99_produce_latency_ms": %d
                }""",
                currentProduceThroughput,
                currentConsumeThroughput,
                getTotalProduced(),
                getTotalConsumed(),
                getTotalErrors(),
                getP99LatencyMs());
    }
}
