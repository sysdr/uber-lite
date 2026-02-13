package com.uberlite;

import com.uberlite.utils.SeedGenerator;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SeedGeneratorTest {
    
    @Test
    void deterministicSeeds_areReproducible() {
        var seed1 = SeedGenerator.deterministicSeed("D-0001", "scenario-1");
        var seed2 = SeedGenerator.deterministicSeed("D-0001", "scenario-1");
        
        assertEquals(seed1, seed2, "Same inputs must produce same seed");
    }
    
    @Test
    void deterministicSeeds_varyByEntityId() {
        var seed1 = SeedGenerator.deterministicSeed("D-0001", "scenario-1");
        var seed2 = SeedGenerator.deterministicSeed("D-0002", "scenario-1");
        
        assertNotEquals(seed1, seed2, "Different entities must have different seeds");
    }
    
    @Test
    void deterministicSeeds_varyByScenario() {
        var seed1 = SeedGenerator.deterministicSeed("D-0001", "scenario-1");
        var seed2 = SeedGenerator.deterministicSeed("D-0001", "scenario-2");
        
        assertNotEquals(seed1, seed2, "Different scenarios must have different seeds");
    }
    
    @Test
    void stochasticSeeds_areUnique() {
        Set<Long> seeds = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            seeds.add(SeedGenerator.stochasticSeed());
        }
        
        assertEquals(1000, seeds.size(), "All stochastic seeds should be unique");
    }
    
    @Test
    void collisionDetection_identifiesDuplicates() {
        long[] withCollision = {123L, 456L, 123L, 789L};
        assertTrue(SeedGenerator.hasCollisions(withCollision));
        
        long[] withoutCollision = {123L, 456L, 789L, 101112L};
        assertFalse(SeedGenerator.hasCollisions(withoutCollision));
    }
    
    @Test
    void seedDistribution_noCollisionsIn10kDrivers() {
        long[] seeds = new long[10_000];
        for (int i = 0; i < seeds.length; i++) {
            seeds[i] = SeedGenerator.deterministicSeed(String.format("D-%05d", i), "scenario-1");
        }
        
        assertFalse(SeedGenerator.hasCollisions(seeds), 
            "Should have no collisions in 10k drivers");
    }
}
