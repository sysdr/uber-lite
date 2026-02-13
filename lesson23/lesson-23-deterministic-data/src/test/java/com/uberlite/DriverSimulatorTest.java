package com.uberlite;

import com.uberlite.simulator.DriverSimulator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DriverSimulatorTest {
    
    @Test
    void deterministicSimulator_producesIdenticalTrajectories() {
        var sim1 = DriverSimulator.createDeterministic("D-TEST", "test-scenario", 37.7749, -122.4194);
        var sim2 = DriverSimulator.createDeterministic("D-TEST", "test-scenario", 37.7749, -122.4194);
        
        var trajectory1 = sim1.tick(10);
        var trajectory2 = sim2.tick(10);
        
        assertEquals(trajectory1, trajectory2, "Deterministic simulators must produce identical trajectories");
    }
    
    @Test
    void deterministicSimulator_differentScenarios_produceDifferentTrajectories() {
        var sim1 = DriverSimulator.createDeterministic("D-TEST", "scenario-A", 37.7749, -122.4194);
        var sim2 = DriverSimulator.createDeterministic("D-TEST", "scenario-B", 37.7749, -122.4194);
        
        var trajectory1 = sim1.tick(10);
        var trajectory2 = sim2.tick(10);
        
        assertNotEquals(trajectory1, trajectory2, "Different scenarios must produce different trajectories");
    }
    
    @Test
    void stochasticSimulator_producesUniqueTrajectories() {
        var sim1 = DriverSimulator.createStochastic("D-TEST", 37.7749, -122.4194);
        var sim2 = DriverSimulator.createStochastic("D-TEST", 37.7749, -122.4194);
        
        var trajectory1 = sim1.tick(10);
        var trajectory2 = sim2.tick(10);
        
        assertNotEquals(trajectory1, trajectory2, "Stochastic simulators must produce different trajectories");
    }
    
    @Test
    void simulator_recordsHistory() {
        var sim = DriverSimulator.createDeterministic("D-TEST", "test", 37.7749, -122.4194);
        
        sim.tick(5);
        var history = sim.getHistory();
        
        assertEquals(6, history.size(), "History should contain initial + 5 updates");
        
        // Verify step numbers
        for (int i = 0; i < history.size(); i++) {
            assertEquals(i, history.get(i).stepNumber());
        }
    }
    
    @Test
    void simulator_snapshotCapturesState() {
        var sim = DriverSimulator.createDeterministic("D-TEST", "test", 37.7749, -122.4194);
        sim.tick(10);
        
        var snapshot = sim.snapshot();
        
        assertEquals("D-TEST", snapshot.driverId());
        assertNotEquals(0, snapshot.h3Index());
        assertNotEquals(0, snapshot.seed());
    }
}
