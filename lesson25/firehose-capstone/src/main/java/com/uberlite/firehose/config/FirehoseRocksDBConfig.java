package com.uberlite.firehose.config;

import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.rocksdb.Options;

import java.util.Map;

/** Uses Kafka Streams default RocksDB config to avoid native library conflicts. */
public class FirehoseRocksDBConfig implements RocksDBConfigSetter {

    @Override
    public void setConfig(String storeName, Options options, Map<String, Object> configs) {}

    @Override
    public void close(String storeName, Options options) {}
}
