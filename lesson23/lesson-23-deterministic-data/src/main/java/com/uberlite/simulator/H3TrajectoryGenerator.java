package com.uberlite.simulator;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class H3TrajectoryGenerator {
    private final H3Core h3;
    private final Random prng;
    private long currentH3;
    private int stepCount;
    
    public H3TrajectoryGenerator(long startH3, Random prng) {
        try {
            this.h3 = H3Core.newInstance();
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
        this.currentH3 = startH3;
        this.prng = prng;
        this.stepCount = 0;
    }
    
    /**
     * Generate next location via deterministic random walk.
     * Stays on H3 grid (simulates road network).
     */
    public long nextLocation() {
        // Get K=1 ring (6 neighbors + self)
        var neighbors = new ArrayList<>(h3.gridDisk(currentH3, 1));
        
        // Remove current cell (force movement)
        neighbors.remove(currentH3);
        
        if (neighbors.isEmpty()) {
            throw new IllegalStateException("No neighbors available");
        }
        
        // Deterministic neighbor selection
        int idx = prng.nextInt(neighbors.size());
        currentH3 = neighbors.get(idx);
        stepCount++;
        
        return currentH3;
    }
    
    /**
     * Get bounded random walk within radius.
     * Uses Gaussian distribution to keep most movement near center.
     */
    public long nextLocationBounded(double centerLat, double centerLng, double radiusKm) {
        try {
            // Generate offset with Gaussian distribution (99.7% within 3 sigma)
            double latOffset = prng.nextGaussian() * (radiusKm / 111.0 / 3); // ~111km per degree
            double lngOffset = prng.nextGaussian() * (radiusKm / 111.0 / 3) / Math.cos(Math.toRadians(centerLat));
            
            double newLat = centerLat + latOffset;
            double newLng = centerLng + lngOffset;
            
            // Clamp to valid ranges
            newLat = Math.max(-90, Math.min(90, newLat));
            newLng = Math.max(-180, Math.min(180, newLng));
            
            currentH3 = h3.latLngToCell(newLat, newLng, 9);
            stepCount++;
            
            return currentH3;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate bounded location", e);
        }
    }
    
    public long getCurrentH3() {
        return currentH3;
    }
    
    public int getStepCount() {
        return stepCount;
    }
    
    /**
     * Fast-forward to specific step number (for replay).
     */
    public void fastForward(int targetStep) {
        while (stepCount < targetStep) {
            nextLocation();
        }
    }
}
