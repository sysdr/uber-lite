package com.uberlite.pathinterpolation;

import com.uber.h3core.H3Core;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PathInterpolationProcessor 
    implements Processor<String, DriverLocationUpdate, String, CellEntry> {
    
    private static final Logger log = LoggerFactory.getLogger(PathInterpolationProcessor.class);
    private static final String STATE_STORE_NAME = "cell-driver-index";
    private static final Duration TTL = Duration.ofSeconds(10);
    private static final Duration PUNCTUATE_INTERVAL = Duration.ofSeconds(5);
    
    private ProcessorContext<String, CellEntry> context;
    private KeyValueStore<String, CellEntry> stateStore;
    private H3Core h3;
    
    // In-memory cache of previous cell per driver
    private final Map<String, Long> driverPreviousCell = new HashMap<>();
    
    // Metrics
    private long pathInterpolationCount = 0;
    private long totalPathLength = 0;
    private long pathFallbackCount = 0;
    
    @Override
    public void init(ProcessorContext<String, CellEntry> context) {
        this.context = context;
        this.stateStore = context.getStateStore(STATE_STORE_NAME);
        
        try {
            this.h3 = H3Core.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
        
        // Schedule punctuator for TTL cleanup
        context.schedule(
            PUNCTUATE_INTERVAL,
            PunctuationType.WALL_CLOCK_TIME,
            this::cleanupStaleEntries
        );
        
        log.info("PathInterpolationProcessor initialized with TTL={}", TTL);
    }
    
    @Override
    public void process(Record<String, DriverLocationUpdate> record) {
        var update = record.value();
        var driverId = update.driverId();
        var currentCell = update.h3Cell();
        var timestamp = update.timestamp();
        
        // Get previous cell for this driver
        var previousCell = driverPreviousCell.get(driverId);
        
        if (previousCell == null) {
            // First update for this driver - just store current position
            writeCellEntry(currentCell, driverId, timestamp, currentCell, false);
            driverPreviousCell.put(driverId, currentCell);
            return;
        }
        
        if (previousCell.equals(currentCell)) {
            // Driver still in same cell - update timestamp
            writeCellEntry(currentCell, driverId, timestamp, currentCell, false);
            return;
        }
        
        // Compute path interpolation
        List<Long> path;
        try {
            path = h3.gridPathCells(previousCell, currentCell);
            
            if (path.isEmpty()) {
                // GridPath failed (cells too far apart) - fallback to endpoint
                log.warn("GridPath failed for driver={}, distance={} cells, using fallback", 
                         driverId, h3.gridDistance(previousCell, currentCell));
                path = List.of(currentCell);
                pathFallbackCount++;
            }
        } catch (Exception e) {
            log.error("GridPath exception for driver={}", driverId, e);
            path = List.of(currentCell);
            pathFallbackCount++;
        }
        
        // Write all cells in path
        for (int i = 0; i < path.size(); i++) {
            var cell = path.get(i);
            var isInterpolated = (i > 0 && i < path.size() - 1); // Middle cells are interpolated
            writeCellEntry(cell, driverId, timestamp, currentCell, isInterpolated);
        }
        
        // Update metrics
        pathInterpolationCount++;
        totalPathLength += path.size();
        
        if (pathInterpolationCount % 1000 == 0) {
            log.info("Path stats: count={}, avg_length={}, fallbacks={}", 
                     pathInterpolationCount, 
                     String.format("%.2f", (double) totalPathLength / pathInterpolationCount),
                     pathFallbackCount);
        }
        
        // Update previous cell
        driverPreviousCell.put(driverId, currentCell);
    }
    
    private void writeCellEntry(long cell, String driverId, long timestamp, 
                                long currentCell, boolean interpolated) {
        var key = cellDriverKey(cell, driverId);
        var entry = new CellEntry(driverId, timestamp, currentCell, interpolated);
        stateStore.put(key, entry);
    }
    
    private String cellDriverKey(long cell, String driverId) {
        return "cell:" + cell + ":driver:" + driverId;
    }
    
    private void cleanupStaleEntries(long timestamp) {
        var cutoff = timestamp - TTL.toMillis();
        var deleted = 0;
        
        try (var iter = stateStore.all()) {
            while (iter.hasNext()) {
                var entry = iter.next();
                if (entry.value.timestamp() < cutoff) {
                    stateStore.delete(entry.key);
                    deleted++;
                }
            }
        }
        
        if (deleted > 0) {
            log.info("Punctuator cleanup: deleted {} stale entries (cutoff={})", 
                     deleted, cutoff);
        }
    }
}
