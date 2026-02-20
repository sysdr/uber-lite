package com.uberlite.lesson27;

import com.uber.h3core.H3Core;
import java.io.IOException;

public final class H3Util {

    public static final int RESOLUTION = 6;

    // H3Core is thread-safe — one instance per JVM is sufficient
    public static final H3Core H3;

    static {
        try {
            H3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Returns the H3 Resolution-6 cell index for a GPS coordinate.
     * Computation time: ~50ns. No I/O. No allocation after JIT warmup.
     */
    public static long cellOf(double lat, double lng) {
        return H3.latLngToCell(lat, lng, RESOLUTION);
    }

    /**
     * Maps an H3 cell index to a Kafka partition number.
     * Uses Long's XOR-fold hash — identical to what Java's HashMap uses
     * for long keys, and deterministic across JVMs for the same input.
     */
    public static int partitionFor(long h3Cell, int numPartitions) {
        int hash = (int)(h3Cell ^ (h3Cell >>> 32));
        return Math.abs(hash) % numPartitions;
    }

    private H3Util() {}
}
