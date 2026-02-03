package com.uberlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe metrics collector for producer performance analysis.
 */
public class MetricsCollector {
    private final Map<Integer, LongAdder> recordsPerPartition = new ConcurrentHashMap<>();
    private final AtomicLong totalRecords = new AtomicLong(0);
    private final AtomicLong totalErrors = new AtomicLong(0);
    private final AtomicLong totalLatencyMs = new AtomicLong(0);
    private final long startTimeMs = System.currentTimeMillis();
    
    public void recordSuccess(int partition, long latencyMs) {
        recordsPerPartition.computeIfAbsent(partition, k -> new LongAdder()).increment();
        totalRecords.incrementAndGet();
        totalLatencyMs.addAndGet(latencyMs);
    }
    
    public void recordError() {
        totalErrors.incrementAndGet();
    }
    
    public Map<Integer, Long> getPartitionDistribution() {
        return recordsPerPartition.entrySet().stream()
            .collect(java.util.stream.Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().sum()
            ));
    }
    
    public double getPartitionCV() {
        var counts = recordsPerPartition.values().stream()
            .mapToLong(LongAdder::sum)
            .toArray();
        
        if (counts.length == 0) return 0.0;
        
        double mean = java.util.Arrays.stream(counts).average().orElse(0);
        double variance = java.util.Arrays.stream(counts)
            .mapToDouble(c -> Math.pow(c - mean, 2))
            .average()
            .orElse(0);
        double stdDev = Math.sqrt(variance);
        
        return mean > 0 ? stdDev / mean : 0.0;
    }
    
    public String toJson() {
        var mapper = new ObjectMapper();
        ObjectNode root = mapper.createObjectNode();
        
        long elapsedMs = System.currentTimeMillis() - startTimeMs;
        long total = totalRecords.get();
        
        root.put("total_records", total);
        root.put("total_errors", totalErrors.get());
        root.put("elapsed_ms", elapsedMs);
        root.put("throughput_per_sec", elapsedMs > 0 ? (total * 1000.0 / elapsedMs) : 0);
        root.put("avg_latency_ms", total > 0 ? (totalLatencyMs.get() / (double) total) : 0);
        root.put("partition_cv", getPartitionCV());
        
        var partitions = mapper.createObjectNode();
        getPartitionDistribution().forEach((p, count) -> 
            partitions.put("partition_" + p, count));
        root.set("partition_distribution", partitions);
        
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(root);
        } catch (Exception e) {
            return "{}";
        }
    }
}
