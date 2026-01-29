package com.uberlite.deps;

import com.uber.h3core.H3Core;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import static org.junit.jupiter.api.Assertions.*;

class NativeDependencyTest {
    
    @Test
    void testRocksDBLibraryLoads() {
        assertDoesNotThrow(() -> {
            RocksDB.loadLibrary();
            String version = RocksDB.rocksdbVersion().toString();
            assertTrue(version.startsWith("9."), 
                      "Expected RocksDB 9.x, got " + version);
        });
    }
    
    @Test
    void testH3LibraryLoads() {
        assertDoesNotThrow(() -> {
            H3Core h3 = H3Core.newInstance();
            long cell = h3.latLngToCell(0, 0, 9);
            assertTrue(h3.isValidCell(cell));
        });
    }
    
    @Test
    void testNoConcurrentNativeLoadingIssues() throws Exception {
        // Simulate multiple threads loading native libs (common in Kafka Streams)
        var thread1 = Thread.ofVirtual().start(() -> {
            try {
                RocksDB.loadLibrary();
            } catch (Exception e) {
                fail("Thread 1 failed: " + e.getMessage());
            }
        });
        
        var thread2 = Thread.ofVirtual().start(() -> {
            try {
                H3Core.newInstance();
            } catch (Exception e) {
                fail("Thread 2 failed: " + e.getMessage());
            }
        });
        
        thread1.join();
        thread2.join();
    }
}
