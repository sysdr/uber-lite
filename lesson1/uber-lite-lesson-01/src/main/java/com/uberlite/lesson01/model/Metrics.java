package com.uberlite.lesson01.model;

public record Metrics(
    long successCount,
    long failureCount,
    double p99LatencyMs,
    double throughput
) {}
