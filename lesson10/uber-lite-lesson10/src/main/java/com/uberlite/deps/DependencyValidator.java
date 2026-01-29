package com.uberlite.deps;

import com.uber.h3core.H3Core;
import org.rocksdb.RocksDB;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Validates that native libraries (RocksDB, H3) load correctly.
 * In production, this runs as a startup health check.
 */
public class DependencyValidator {
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Dependency Validation Started ===\n");
        
        validatePlatform();
        validateRocksDB();
        validateH3();
        
        System.out.println("\n=== All Validations Passed ===");
    }
    
    private static void validatePlatform() {
        String os = System.getProperty("os.name").toLowerCase();
        String arch = System.getProperty("os.arch");
        
        System.out.printf("Platform: %s / %s%n", os, arch);
        
        // Production systems enforce Linux x86_64
        if (System.getenv("ENFORCE_PLATFORM") != null) {
            if (!os.contains("linux") || !arch.equals("amd64")) {
                throw new IllegalStateException(
                    "Unsupported platform - expected linux/amd64, got " + os + "/" + arch
                );
            }
        }
        System.out.println("✓ Platform validation passed");
    }
    
    private static void validateRocksDB() throws RocksDBException, IOException {
        System.out.println("\nValidating RocksDB...");
        
        // Trigger native library load
        RocksDB.loadLibrary();
        
        // Verify version matches build config
        String version = RocksDB.rocksdbVersion().toString();
        System.out.printf("RocksDB version: %s%n", version);
        
        // Test basic functionality
        Path tempDir = Files.createTempDirectory("rocksdb-test");
        try (Options options = new Options().setCreateIfMissing(true);
             RocksDB db = RocksDB.open(options, tempDir.toString())) {
            
            db.put("test-key".getBytes(), "test-value".getBytes());
            byte[] value = db.get("test-key".getBytes());
            
            if (!new String(value).equals("test-value")) {
                throw new IllegalStateException("RocksDB read/write test failed");
            }
            
            System.out.println("✓ RocksDB functional test passed");
        } finally {
            // Cleanup
            deleteDirectory(tempDir.toFile());
        }
    }
    
    private static void validateH3() throws IOException {
        System.out.println("\nValidating H3...");
        
        // Trigger native library load
        H3Core h3 = H3Core.newInstance();
        
        // Test basic functionality
        double lat = 40.7128;  // NYC
        double lng = -74.0060;
        int resolution = 9;
        
        long h3Index = h3.latLngToCell(lat, lng, resolution);
        System.out.printf("H3 cell for NYC (%.4f, %.4f) at res %d: %016x%n", 
                         lat, lng, resolution, h3Index);
        
        // Verify the index is valid
        if (!h3.isValidCell(h3Index)) {
            throw new IllegalStateException("H3 generated invalid cell index");
        }
        
        // Test K-ring computation (requires native code)
        var neighbors = h3.gridDisk(h3Index, 1);
        if (neighbors.size() != 7) {  // Center + 6 neighbors
            throw new IllegalStateException(
                "H3 K-ring computation failed - expected 7 cells, got " + neighbors.size()
            );
        }
        
        System.out.println("✓ H3 functional test passed");
    }
    
    private static void deleteDirectory(java.io.File dir) {
        if (dir.isDirectory()) {
            for (java.io.File file : dir.listFiles()) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }
}
