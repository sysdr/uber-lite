package com.uberlite.firehose.topology;

import com.uber.h3core.H3Core;
import com.uberlite.firehose.metrics.MetricsRegistry;
import com.uberlite.firehose.model.DriverLocationEvent;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

/**
 * Processor API (PAPI) processor — no DSL abstractions.
 *
 * State schema (RocksDB):
 *   Key:   driverId as String (e.g. "00042")
 *   Value: H3 cell index as Long (Resolution 9)
 *
 * Punctuator fires every 1 second on WALL_CLOCK_TIME (works even during idle
 * periods, unlike STREAM_TIME which freezes when no records arrive).
 */
public class DriverLocationProcessor implements Processor<String, byte[], Void, Void> {

    private static final Logger log = LoggerFactory.getLogger(DriverLocationProcessor.class);
    public  static final String STORE_NAME = "driver-cell-store";

    private ProcessorContext<Void, Void>    context;
    private KeyValueStore<String, Long>     driverCellStore;
    private final MetricsRegistry           metrics;

    // Per-processor H3Core instance — initialized in init(), reused for all records
    // on this StreamThread. Avoids JNI bridge allocation on the hot path.
    private H3Core h3;

    // Window counter for consume-throughput measurement
    private long windowProcessed = 0;

    public DriverLocationProcessor(MetricsRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext<Void, Void> context) {
        this.context        = context;
        this.driverCellStore = context.getStateStore(STORE_NAME);

        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3Core in processor", e);
        }

        // WALL_CLOCK_TIME: punctuates every second regardless of record flow
        context.schedule(Duration.ofSeconds(1), PunctuationType.WALL_CLOCK_TIME, ts -> {
            metrics.setConsumeThroughput(windowProcessed);
            log.info("[PROCESSOR task={}] {}/sec | total_consumed={}",
                     context.taskId(), windowProcessed, metrics.getTotalConsumed());
            windowProcessed = 0;
        });
    }

    @Override
    public void process(Record<String, byte[]> record) {
        var event = DriverLocationEvent.deserialize(record.value());

        // Compute H3 cell at Resolution 9 (~0.1 km² hexagonal area)
        long h3Cell = h3.latLngToCell(event.lat(), event.lon(), 9);

        // Write driver → cell mapping into RocksDB
        // Key format: zero-padded driver ID string for lexicographic ordering
        var driverKey = String.format("%05d", event.driverId());
        driverCellStore.put(driverKey, h3Cell);

        metrics.recordConsumed();
        windowProcessed++;
    }

    @Override
    public void close() {
        log.info("[PROCESSOR task={}] closing, final consumed this session: {}",
                 context.taskId(), metrics.getTotalConsumed());
    }
}
