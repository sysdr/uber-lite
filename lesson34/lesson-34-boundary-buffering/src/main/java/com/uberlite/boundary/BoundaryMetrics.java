package com.uberlite.boundary;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared atomic counters for boundary buffering metrics.
 * Exposed via MetricsServer on HTTP :7072.
 */
public final class BoundaryMetrics {
    public final AtomicLong totalProcessed  = new AtomicLong(0);
    public final AtomicLong boundaryHits    = new AtomicLong(0);
    public final AtomicLong multicastCopies = new AtomicLong(0);
    public final AtomicLong dedupDrops      = new AtomicLong(0);

    /** Returns ratio of boundary-multicast events to total. Target: < 0.15 */
    public double amplificationRatio() {
        long total = totalProcessed.get();
        return total == 0 ? 0.0 : (double) multicastCopies.get() / total;
    }
}
