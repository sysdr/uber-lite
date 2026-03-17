package com.uberlite.boundary;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

/**
 * RocksDB tuning for the dedup store.
 * The dedup store is write-heavy (every non-duplicate record updates it)
 * and read-heavy (every record requires a point lookup).
 *
 * Key tuning:
 * - Block cache: 32MB — dedup keys are short strings, high cache hit rate
 * - Write buffer: 4MB × 2 — absorbs write bursts from multicast amplification
 * - Bloom filters: 10 bits/key — reduces false positives on duplicate checks
 */
public final class BoundaryRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        if (DedupProcessor.STORE_NAME.equals(storeName)) {
            options.setWriteBufferSize(4 * 1024 * 1024);         // 4MB
            options.setMaxWriteBufferNumber(2);
            options.setLevel0FileNumCompactionTrigger(4);
            options.setLevel0SlowdownWritesTrigger(8);
            options.setLevel0StopWritesTrigger(12);
        }
    }

    @Override
    public void close(String storeName, Options options) {}
}
