package com.uberlite;

public record LocationKey(String entityId, long h3Index) {
    @Override
    public String toString() {
        return "LocationKey[entityId=" + entityId + ", h3=" + Long.toHexString(h3Index) + "]";
    }
}
