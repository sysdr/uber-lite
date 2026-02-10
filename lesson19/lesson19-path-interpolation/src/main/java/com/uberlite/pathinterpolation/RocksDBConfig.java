package com.uberlite.pathinterpolation;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

public class RocksDBConfig implements RocksDBConfigSetter {
    
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        // Optimize for range scans
        options.setCompactionStyle(org.rocksdb.CompactionStyle.LEVEL);
        options.setLevel0FileNumCompactionTrigger(4);
        
        // Block cache for read performance
        var blockCache = new org.rocksdb.LRUCache(64 * 1024 * 1024); // 64MB
        var tableConfig = new org.rocksdb.BlockBasedTableConfig();
        tableConfig.setBlockCache(blockCache);
        tableConfig.setBlockSize(16 * 1024); // 16KB blocks
        options.setTableFormatConfig(tableConfig);
        
        // Write buffer
        options.setWriteBufferSize(16 * 1024 * 1024); // 16MB
        options.setMaxWriteBufferNumber(3);
        
        // Compression
        options.setCompressionType(org.rocksdb.CompressionType.LZ4_COMPRESSION);
    }
    
    @Override
    public void close(String storeName, Options options) {
        // Cleanup handled by Kafka Streams
    }
}
