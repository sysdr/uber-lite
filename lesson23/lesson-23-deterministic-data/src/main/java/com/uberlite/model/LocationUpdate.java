package com.uberlite.model;

public record LocationUpdate(
    String entityId,
    long h3Index,
    long timestamp,
    int stepNumber
) {}
