package com.uberlite.geohash.model;

public record PartitionMetric(
    String partitionerType, // "geohash" or "h3"
    int partition,
    int latitudeBand,
    long recordCount,
    long timestamp
) {}
