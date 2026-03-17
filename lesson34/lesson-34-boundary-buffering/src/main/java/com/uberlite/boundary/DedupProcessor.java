package com.uberlite.boundary;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Deduplicates driver location events using a RocksDB-backed KeyValueStore.
 *
 * Dedup key:   driverId (String)
 * Dedup value: last-seen eventTimestamp (Long)
 *
 * A record is a duplicate if its eventTimestamp == stored timestamp for that driverId.
 * (Two multicasted copies of the same event have identical driverId + eventTimestamp.)
 *
 * TTL cleanup: Punctuator at WALL_CLOCK_TIME every 30s removes entries older than DEDUP_WINDOW_MS.
 * This prevents unbounded RocksDB growth without requiring native TTL configuration.
 */
public class DedupProcessor
        implements Processor<String, DriverLocationEvent, String, DriverLocationEvent> {

    private static final Logger log = LoggerFactory.getLogger(DedupProcessor.class);

    public static final String STORE_NAME = "dedup-store";
    public static final long   DEDUP_WINDOW_MS = 10_000L; // 10 seconds
    private static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(30);

    private ProcessorContext<String, DriverLocationEvent> context;
    private KeyValueStore<String, Long> dedupStore;
    private final BoundaryMetrics metrics;

    public DedupProcessor(BoundaryMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, DriverLocationEvent> context) {
        this.context = context;
        this.dedupStore = context.getStateStore(STORE_NAME);

        // Schedule TTL cleanup at wall-clock time (survives consumer lag / paused partitions)
        context.schedule(PUNCTUATE_INTERVAL, PunctuationType.WALL_CLOCK_TIME, this::cleanExpiredEntries);

        log.info("DedupProcessor initialised. Store: {}", STORE_NAME);
    }

    @Override
    public void process(Record<String, DriverLocationEvent> record) {
        var event = record.value();
        String deduKey = event.driverId();
        Long storedTs = dedupStore.get(deduKey);

        if (storedTs != null && storedTs.equals(event.eventTimestamp())) {
            // Duplicate: same driver, same event timestamp — a multicast copy
            metrics.dedupDrops.incrementAndGet();
            log.debug("Dropped duplicate: driverId={} ts={}", event.driverId(), event.eventTimestamp());
            return;
        }

        // Not a duplicate — update store and forward
        dedupStore.put(deduKey, event.eventTimestamp());
        context.forward(record);
    }

    /**
     * Punctuator: evict entries whose last-seen timestamp is older than DEDUP_WINDOW_MS.
     * Runs at wall-clock time to avoid dependency on event-time watermarks.
     */
    private void cleanExpiredEntries(long wallClockMs) {
        long cutoff = wallClockMs - DEDUP_WINDOW_MS;
        List<String> toDelete = new ArrayList<>();

        try (KeyValueIterator<String, Long> it = dedupStore.all()) {
            while (it.hasNext()) {
                var entry = it.next();
                if (entry.value != null && entry.value < cutoff) {
                    toDelete.add(entry.key);
                }
            }
        }

        toDelete.forEach(dedupStore::delete);
        if (!toDelete.isEmpty()) {
            log.debug("Dedup TTL sweep: evicted {} entries older than {}ms", toDelete.size(), DEDUP_WINDOW_MS);
        }
    }

    @Override
    public void close() {
        log.info("DedupProcessor closed. dedupDrops={}", metrics.dedupDrops.get());
    }
}
