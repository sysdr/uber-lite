package com.uberlite.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class SpatialMetrics implements SpatialMetricsMBean {
    private final AtomicLong res9Count = new AtomicLong();
    private final AtomicLong res10Count = new AtomicLong();
    private final AtomicLong totalLocations = new AtomicLong();
    
    public void recordResolution(int resolution) {
        totalLocations.incrementAndGet();
        if (resolution == 9) {
            res9Count.incrementAndGet();
        } else if (resolution == 10) {
            res10Count.incrementAndGet();
        }
    }
    
    @Override
    public long getRes9Count() {
        return res9Count.get();
    }
    
    @Override
    public long getRes10Count() {
        return res10Count.get();
    }
    
    @Override
    public long getTotalLocations() {
        return totalLocations.get();
    }
    
    @Override
    public double getAverageH3Resolution() {
        long total = totalLocations.get();
        if (total == 0) return 0.0;
        return (res9Count.get() * 9.0 + res10Count.get() * 10.0) / total;
    }
}
