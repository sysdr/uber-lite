package com.uberlite.lesson01.kafka;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

public class CustomRocksDbConfig implements RocksDBConfigSetter {
    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {
        options.setWriteBufferSize(64 * 1024 * 1024L);  // 64MB
        options.setMaxWriteBufferNumber(3);
        options.setCompressionType(org.rocksdb.CompressionType.SNAPPY_COMPRESSION);
    }

    @Override
    public void close(String storeName, Options options) {
        // No-op
    }
}
