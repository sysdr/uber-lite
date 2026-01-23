package com.uberlite.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uberlite.model.DriverLocation;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.common.utils.Bytes;

import java.nio.ByteBuffer;

public class SpatialIndexProcessor implements Processor<Long, DriverLocation, Void, Void> {
    private KeyValueStore<Bytes, byte[]> store;
    private ProcessorContext<Void, Void> context;
    private final ObjectMapper mapper = new ObjectMapper();
    
    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.context = context;
        this.store = context.getStateStore("spatial-index");
    }
    
    @Override
    public void process(org.apache.kafka.streams.processor.api.Record<Long, DriverLocation> record) {
        if (record.value() == null) return;
        
        long h3Index = record.key();
        DriverLocation driver = record.value();
        
        // Composite key: [h3_index (8 bytes)][driver_id (8 bytes)]
        byte[] key = ByteBuffer.allocate(16)
            .putLong(h3Index)
            .putLong(driver.driverId())
            .array();
        
        try {
            byte[] value = mapper.writeValueAsBytes(driver);
            store.put(Bytes.wrap(key), value);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize driver", e);
        }
    }
}
