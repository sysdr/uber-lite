package com.uberlite.stress;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.*;

import java.util.Map;

/**
 * Explicit RocksDB tuning to prevent unbounded memory growth under 1-hour sustained load.
 *
 * Without this, the default RocksDB block cache grows to consume 50%+ of available heap
 * as the state store accumulates 18M+ entries over a 60-minute test run.
 */
public class BoundedRocksDBConfig implements RocksDBConfigSetter {

    private static final long BLOCK_CACHE_BYTES  = 128L * 1024 * 1024;  // 128MB hard ceiling
    private static final long WRITE_BUFFER_BYTES = 8L   * 1024 * 1024;  // 8MB memtable
    private static final int  MAX_WRITE_BUFFERS  = 2;
    private static final int  MAX_OPEN_FILES     = 100;

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        // Use the BlockBasedTableConfig from Options (required by Kafka Streams 3.6+)
        var blockConfig = (BlockBasedTableConfig) options.tableFormatConfig();
        if (blockConfig != null) {
            blockConfig.setBlockCache(new LRUCache(BLOCK_CACHE_BYTES));
            blockConfig.setBlockSize(16 * 1024);   // 16KB blocks
            blockConfig.setCacheIndexAndFilterBlocks(true);
            blockConfig.setFilterPolicy(new BloomFilter(10, false));
        }
        options.setWriteBufferSize(WRITE_BUFFER_BYTES);
        options.setMaxWriteBufferNumber(MAX_WRITE_BUFFERS);
        options.setCompactionStyle(CompactionStyle.LEVEL);
        options.setLevel0FileNumCompactionTrigger(4);
        options.setMaxOpenFiles(MAX_OPEN_FILES);
    }

    @Override
    public void close(String storeName, Options options) {
        // RocksDB native resources are managed by Options lifecycle
    }
}
