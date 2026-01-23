package com.uberlite;

import org.rocksdb.*;

import java.util.Map;

public class RocksDBConfigSetter implements org.apache.kafka.streams.state.RocksDBConfigSetter {
    
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        
        // Optimize for range queries
        tableConfig.setBlockSize(16 * 1024);  // 16KB blocks for better prefix scans
        tableConfig.setBlockCache(new LRUCache(256 * 1024 * 1024));  // 256MB cache
        tableConfig.setFilterPolicy(null);  // Disable bloom filters for range queries
        
        options.setTableFormatConfig(tableConfig);
        options.setCompressionType(CompressionType.LZ4_COMPRESSION);
        options.setMaxWriteBufferNumber(3);
        options.setWriteBufferSize(64 * 1024 * 1024);  // 64MB write buffer
    }
    
    @Override
    public void close(String storeName, Options options) {
        // Options are managed by Kafka Streams
    }
}
