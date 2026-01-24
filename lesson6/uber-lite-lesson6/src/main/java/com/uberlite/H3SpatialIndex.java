package com.uberlite;

import com.uber.h3core.H3Core;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static com.uberlite.Models.*;

public class H3SpatialIndex implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(H3SpatialIndex.class);
    private static final int H3_RESOLUTION = 9; // ~0.1 km² hexagons
    private static final int SEARCH_RING_SIZE = 2; // K=2 ring (37 cells)
    
    private final RocksDB rocksDB;
    private final H3Core h3;
    
    public H3SpatialIndex(String dbPath) throws RocksDBException, IOException {
        RocksDB.loadLibrary();
        
        var options = new Options()
            .setCreateIfMissing(true)
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            // Tune for write-heavy workload
            .setMaxWriteBufferNumber(4)
            .setWriteBufferSize(64 * 1024 * 1024) // 64MB memtables
            .setMaxBackgroundJobs(4);
        
        this.rocksDB = RocksDB.open(options, dbPath);
        this.h3 = H3Core.newInstance();
        
        log.info("H3SpatialIndex initialized at resolution {}", H3_RESOLUTION);
    }
    
    public void indexDriverLocation(DriverLocationUpdate update) throws RocksDBException {
        long h3Index = h3.latLngToCell(update.lat(), update.lon(), H3_RESOLUTION);
        
        // Key: h3_index (8 bytes) | driver_id_hash (8 bytes)
        byte[] key = ByteBuffer.allocate(16)
            .putLong(h3Index)
            .putLong(hashDriverId(update.driverId()))
            .array();
        
        // Value: lat (8) | lon (8) | timestamp (8) = 24 bytes
        byte[] value = ByteBuffer.allocate(24)
            .putDouble(update.lat())
            .putDouble(update.lon())
            .putLong(update.timestamp())
            .array();
        
        rocksDB.put(key, value);
    }
    
    public List<DriverLocation> findNearbyDrivers(double riderLat, double riderLon) 
            throws RocksDBException {
        long startTime = System.nanoTime();
        
        // Get H3 cell for rider
        long riderH3 = h3.latLngToCell(riderLat, riderLon, H3_RESOLUTION);
        
        // Compute K-ring neighbors
        List<Long> searchCells = h3.gridDisk(riderH3, SEARCH_RING_SIZE);
        
        log.debug("Searching {} H3 cells (K={} ring)", searchCells.size(), SEARCH_RING_SIZE);
        
        var candidates = new ArrayList<DriverLocation>();
        int totalKeysScanned = 0;
        
        for (long cellIndex : searchCells) {
            byte[] prefix = ByteBuffer.allocate(8).putLong(cellIndex).array();
            
            try (RocksIterator iter = rocksDB.newIterator()) {
                iter.seek(prefix);
                
                while (iter.isValid() && startsWith(iter.key(), prefix)) {
                    totalKeysScanned++;
                    
                    var location = deserializeDriverLocation(iter.key(), iter.value());
                    candidates.add(location);
                    
                    iter.next();
                }
            }
        }
        
        long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
        log.info("Found {} drivers in {}ms (scanned {} keys across {} cells)",
            candidates.size(), elapsedMs, totalKeysScanned, searchCells.size());
        
        return candidates;
    }
    
    private boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) return false;
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) return false;
        }
        return true;
    }
    
    private long hashDriverId(String driverId) {
        // Simple hash for demo - production would use consistent hashing
        return driverId.hashCode() & 0xFFFFFFFFL;
    }
    
    private DriverLocation deserializeDriverLocation(byte[] key, byte[] value) {
        // Extract driver ID hash from key (bytes 8-15)
        long driverIdHash = ByteBuffer.wrap(key, 8, 8).getLong();
        
        // Parse value
        var buf = ByteBuffer.wrap(value);
        double lat = buf.getDouble();
        double lon = buf.getDouble();
        long timestamp = buf.getLong();
        
        return new DriverLocation(
            "driver_" + driverIdHash,
            lat, lon, timestamp
        );
    }
    
    @Override
    public void close() throws Exception {
        if (rocksDB != null) {
            rocksDB.close();
        }
    }
}
