package com.uberlite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class H3SpatialIndexApp {
    private static final Logger log = LoggerFactory.getLogger(H3SpatialIndexApp.class);
    
    public static void main(String[] args) throws Exception {
        log.info("Starting H3 Spatial Index Application");
        
        var spatialIndex = new H3SpatialIndex("/tmp/h3-spatial-index");
        var driverConsumer = new DriverLocationConsumer("localhost:9092", spatialIndex);
        var matchHandler = new MatchRequestHandler("localhost:9092", spatialIndex);
        
        // Start consumers
        driverConsumer.start();
        matchHandler.start();
        
        log.info("Application running. Press Ctrl+C to stop.");
        
        // Keep alive
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("Shutting down...");
                driverConsumer.close();
                matchHandler.close();
                spatialIndex.close();
            } catch (Exception e) {
                log.error("Error during shutdown", e);
            }
        }));
        
        Thread.currentThread().join();
    }
}
