package com.uberlite.boundary;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Processor API: reads raw driver location events, detects H3 boundary proximity,
 * and emits to primary + (if near boundary) neighboring Res-5 partition keys.
 *
 * This processor is stateless — boundary detection is purely positional.
 * State lives only in the downstream DedupProcessor.
 *
 * Forwarded records use compound key: "res5CellAddress|subShard"
 * This ensures they land on the correct partition via the compound partitioner (Lesson 32).
 */
public class BoundaryBufferingProcessor
        implements Processor<String, DriverLocationEvent, String, DriverLocationEvent> {

    private static final Logger log = LoggerFactory.getLogger(BoundaryBufferingProcessor.class);

    private ProcessorContext<String, DriverLocationEvent> context;
    private final BoundaryDetector detector;
    private final BoundaryMetrics metrics;

    public BoundaryBufferingProcessor(BoundaryDetector detector, BoundaryMetrics metrics) {
        this.detector = detector;
        this.metrics = metrics;
    }

    @Override
    public void init(ProcessorContext<String, DriverLocationEvent> context) {
        this.context = context;
        log.info("BoundaryBufferingProcessor initialised on task {}", context.taskId());
    }

    @Override
    public void process(Record<String, DriverLocationEvent> record) {
        var event = record.value();
        metrics.totalProcessed.incrementAndGet();

        BoundaryDetectionResult result = detector.detectTargetCells(event.lat(), event.lng());

        for (long res5Cell : result.allCells()) {
            // Build compound partition key for this target cell
            String newKey = BoundaryDetector.toPartitionKey(res5Cell, event.driverId());
            context.forward(record.withKey(newKey));
        }

        if (result.nearBoundary()) {
            metrics.boundaryHits.incrementAndGet();
            // Count extra copies (allCells.size() - 1 = number of duplicates)
            metrics.multicastCopies.addAndGet(result.allCells().size() - 1);
            log.debug("Multicast driverId={} to {} cells: {}",
                    event.driverId(), result.allCells().size(), result.allCells());
        }
    }

    @Override
    public void close() {
        log.info("BoundaryBufferingProcessor closed. totalProcessed={}, boundaryHits={}, copies={}",
                metrics.totalProcessed.get(),
                metrics.boundaryHits.get(),
                metrics.multicastCopies.get());
    }
}
