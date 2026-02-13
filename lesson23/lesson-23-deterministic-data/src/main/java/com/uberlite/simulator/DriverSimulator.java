package com.uberlite.simulator;

import com.uber.h3core.H3Core;
import com.uberlite.model.DriverState;
import com.uberlite.model.LocationUpdate;
import com.uberlite.utils.SeedGenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DriverSimulator {
    private final String driverId;
    private final long seed;
    private final H3TrajectoryGenerator trajectory;
    private final List<LocationUpdate> history;
    
    private DriverSimulator(String driverId, long seed, long startH3) {
        this.driverId = driverId;
        this.seed = seed;
        this.trajectory = new H3TrajectoryGenerator(startH3, new Random(seed));
        this.history = new ArrayList<>();
        
        // Record initial position
        history.add(new LocationUpdate(driverId, startH3, System.currentTimeMillis(), 0));
    }
    
    /**
     * Create simulator in deterministic mode (for tests).
     */
    public static DriverSimulator createDeterministic(String driverId, String scenario, double lat, double lng) {
        try {
            var h3 = H3Core.newInstance();
            long startH3 = h3.latLngToCell(lat, lng, 9);
            long seed = SeedGenerator.deterministicSeed(driverId, scenario);
            return new DriverSimulator(driverId, seed, startH3);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }
    
    /**
     * Create simulator in stochastic mode (for production).
     */
    public static DriverSimulator createStochastic(String driverId, double lat, double lng) {
        try {
            var h3 = H3Core.newInstance();
            long startH3 = h3.latLngToCell(lat, lng, 9);
            long seed = SeedGenerator.stochasticSeed();
            return new DriverSimulator(driverId, seed, startH3);
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize H3", e);
        }
    }
    
    /**
     * Generate next location update.
     */
    public LocationUpdate tick() {
        long newH3 = trajectory.nextLocation();
        var update = new LocationUpdate(
            driverId,
            newH3,
            System.currentTimeMillis(),
            trajectory.getStepCount()
        );
        history.add(update);
        return update;
    }
    
    /**
     * Generate multiple updates (batch).
     */
    public List<LocationUpdate> tick(int count) {
        var updates = new ArrayList<LocationUpdate>(count);
        for (int i = 0; i < count; i++) {
            updates.add(tick());
        }
        return updates;
    }
    
    /**
     * Replay to specific step (for debugging).
     */
    public void replayToStep(int targetStep) {
        history.clear();
        trajectory.fastForward(targetStep);
    }
    
    public String getDriverId() {
        return driverId;
    }
    
    public long getSeed() {
        return seed;
    }
    
    public List<LocationUpdate> getHistory() {
        return List.copyOf(history);
    }
    
    public DriverState snapshot() {
        return new DriverState(
            driverId,
            trajectory.getCurrentH3(),
            java.time.Instant.now(),
            seed
        );
    }
}
