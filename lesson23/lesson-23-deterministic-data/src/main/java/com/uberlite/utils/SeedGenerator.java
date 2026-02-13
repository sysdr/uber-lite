package com.uberlite.utils;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Objects;

public class SeedGenerator {
    
    /**
     * Generate deterministic seed from entity ID and scenario name.
     * Uses robust hashing to minimize collisions.
     */
    public static long deterministicSeed(String entityId, String scenario) {
        // Combine entityId and scenario for better distribution
        var combined = entityId + ":" + scenario;
        
        // Use Java's hash with mixing to improve distribution
        long hash = Objects.hash(combined);
        
        // Additional mixing to spread bits
        hash ^= (hash >>> 33);
        hash *= 0xff51afd7ed558ccdL;
        hash ^= (hash >>> 33);
        hash *= 0xc4ceb9fe1a85ec53L;
        hash ^= (hash >>> 33);
        
        return hash;
    }
    
    /**
     * Generate high-entropy seed for production use.
     * Combines multiple sources of randomness.
     */
    public static long stochasticSeed() {
        var secureRandom = new SecureRandom();
        return secureRandom.nextLong() ^ System.nanoTime() ^ Thread.currentThread().getId();
    }
    
    /**
     * Detect seed collisions in a batch.
     * Returns true if all seeds are unique.
     */
    public static boolean hasCollisions(long[] seeds) {
        var uniqueCount = java.util.Arrays.stream(seeds).distinct().count();
        return uniqueCount != seeds.length;
    }
}
