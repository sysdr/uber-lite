package com.uberlite.compression;

/**
 * Metrics captured for each compression codec benchmark run
 */
public record CompressionMetrics(
    String codec,
    long eventsCount,
    long uncompressedBytes,
    long compressedBytes,
    double compressionRatio,
    long durationNanos,
    long eventsPerSec,
    long networkBytesPerSec
) {
    public static CompressionMetrics calculate(
        String codec,
        long eventsCount,
        long uncompressedBytes,
        long compressedBytes,
        long durationNanos
    ) {
        var compressionRatio = (double) uncompressedBytes / compressedBytes;
        var eventsPerSec = (eventsCount * 1_000_000_000L) / durationNanos;
        var networkBytesPerSec = (compressedBytes * 1_000_000_000L) / durationNanos;
        
        return new CompressionMetrics(
            codec,
            eventsCount,
            uncompressedBytes,
            compressedBytes,
            compressionRatio,
            durationNanos,
            eventsPerSec,
            networkBytesPerSec
        );
    }

    public void printSummary() {
        System.out.printf(
            "Codec: %-8s | Compression: %.2fx | Throughput: %,d events/sec | Network: %.2f MB/sec%n",
            codec,
            compressionRatio,
            eventsPerSec,
            networkBytesPerSec / (1024.0 * 1024.0)
        );
    }

    public double networkReductionPercent() {
        return (1.0 - (1.0 / compressionRatio)) * 100.0;
    }
}
