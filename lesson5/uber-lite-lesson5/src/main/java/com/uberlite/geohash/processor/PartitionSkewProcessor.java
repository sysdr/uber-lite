package com.uberlite.geohash.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.geohash.model.DriverLocation;
import com.uberlite.geohash.model.PartitionMetric;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionSkewProcessor implements Processor<String, byte[], String, byte[]> {
    private ProcessorContext<String, byte[]> context;
    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ObjectMapper mapper = new ObjectMapper();
    private final String partitionerType;

    public PartitionSkewProcessor(String partitionerType) {
        this.partitionerType = partitionerType;
    }

    @Override
    public void init(ProcessorContext<String, byte[]> context) {
        this.context = context;
        
        // Schedule punctuator to emit metrics every 5 seconds
        context.schedule(
            java.time.Duration.ofSeconds(5),
            org.apache.kafka.streams.processor.PunctuationType.WALL_CLOCK_TIME,
            timestamp -> flushMetrics()
        );
    }

    @Override
    public void process(Record<String, byte[]> record) {
        try {
            var location = mapper.readValue(record.value(), DriverLocation.class);
            int partition = context.recordMetadata().get().partition();
            int latBand = location.latitudeBand();
            
            String key = partition + "_" + latBand;
            counters.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
        } catch (Exception e) {
            // Skip malformed records
        }
    }

    private void flushMetrics() {
        try {
            for (var entry : counters.entrySet()) {
                String[] parts = entry.getKey().split("_");
                int partition = Integer.parseInt(parts[0]);
                int latBand = Integer.parseInt(parts[1]);
                
                var metric = new PartitionMetric(
                    partitionerType,
                    partition,
                    latBand,
                    entry.getValue().get(),
                    System.currentTimeMillis()
                );
                
                byte[] value = mapper.writeValueAsBytes(metric);
                context.forward(new Record<>(partitionerType + "_" + entry.getKey(), value, System.currentTimeMillis()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
