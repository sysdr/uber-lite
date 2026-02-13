package com.uberlite.simulator;

import com.uberlite.model.LocationUpdate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DeterministicSimulator {
    
    public static void main(String[] args) {
        // Parse arguments
        boolean isDeterministic = parseArg(args, "--mode", "deterministic").equals("deterministic");
        int driverCount = Integer.parseInt(parseArg(args, "--drivers", "10"));
        int steps = Integer.parseInt(parseArg(args, "--steps", "100"));
        
        System.out.println("🚀 Starting Deterministic Data Simulator");
        System.out.println("   Mode: " + (isDeterministic ? "DETERMINISTIC" : "STOCHASTIC"));
        System.out.println("   Drivers: " + driverCount);
        System.out.println("   Steps: " + steps);
        
        // Create simulators
        var simulators = new ArrayList<DriverSimulator>();
        
        // San Francisco downtown coordinates
        double baseLat = 37.7749;
        double baseLng = -122.4194;
        
        for (int i = 0; i < driverCount; i++) {
            String driverId = String.format("D-%04d", i);
            
            // Slight offset per driver (spread across ~5km radius)
            double lat = baseLat + (i % 10) * 0.01;
            double lng = baseLng + (i / 10) * 0.01;
            
            var simulator = isDeterministic
                ? DriverSimulator.createDeterministic(driverId, "scenario-1", lat, lng)
                : DriverSimulator.createStochastic(driverId, lat, lng);
            
            simulators.add(simulator);
        }
        
        System.out.println("✅ Created " + simulators.size() + " simulators");
        
        // Run simulation with virtual threads
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            long startTime = System.currentTimeMillis();
            
            for (var sim : simulators) {
                executor.submit(() -> {
                    var updates = sim.tick(steps);
                    
                    // Print sample output
                    if (sim.getDriverId().equals("D-0000") || sim.getDriverId().equals("D-0001")) {
                        System.out.printf("Driver %s (seed=%d):%n", sim.getDriverId(), sim.getSeed());
                        updates.stream().limit(5).forEach(u -> 
                            System.out.printf("  Step %d: H3=%016x%n", u.stepNumber(), u.h3Index())
                        );
                    }
                    
                    return updates;
                });
            }
            
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            
            long elapsed = System.currentTimeMillis() - startTime;
            long totalEvents = (long) driverCount * steps;
            double throughput = totalEvents / (elapsed / 1000.0);
            
            System.out.printf("%n📊 Statistics:%n");
            System.out.printf("   Total events: %d%n", totalEvents);
            System.out.printf("   Elapsed: %d ms%n", elapsed);
            System.out.printf("   Throughput: %.0f events/sec%n", throughput);
            
            // Verify determinism if in deterministic mode
            if (isDeterministic) {
                System.out.printf("%n🔍 Verifying determinism...%n");
                verifyDeterminism(simulators);
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Simulation interrupted");
        }
    }
    
    private static void verifyDeterminism(List<DriverSimulator> simulators) {
        // Check that driver D-0000 always produces same trajectory
        var firstDriver = simulators.stream()
            .filter(s -> s.getDriverId().equals("D-0000"))
            .findFirst()
            .orElseThrow();
        
        var trajectory1 = firstDriver.getHistory();
        
        // Recreate same driver and verify identical output
        var verifyDriver = DriverSimulator.createDeterministic("D-0000", "scenario-1", 37.7749, -122.4194);
        verifyDriver.tick(trajectory1.size() - 1); // -1 for initial position
        var trajectory2 = verifyDriver.getHistory();
        
        var path1 = trajectory1.stream().map(u -> u.stepNumber() + ":" + u.h3Index()).toList();
        var path2 = trajectory2.stream().map(u -> u.stepNumber() + ":" + u.h3Index()).toList();
        boolean identical = path1.equals(path2);
        
        if (identical) {
            System.out.println("   ✅ PASS: Trajectories are identical");
        } else {
            System.out.println("   ❌ FAIL: Trajectories differ");
            System.out.println("      Run 1 first 3 cells: " + 
                trajectory1.stream().limit(3).map(u -> String.format("%016x", u.h3Index())).toList());
            System.out.println("      Run 2 first 3 cells: " + 
                trajectory2.stream().limit(3).map(u -> String.format("%016x", u.h3Index())).toList());
        }
    }
    
    private static String parseArg(String[] args, String flag, String defaultValue) {
        String prefix = flag + "=";
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals(flag) && i + 1 < args.length) {
                return args[i + 1];
            }
            if (args[i].startsWith(prefix)) {
                return args[i].substring(prefix.length());
            }
        }
        return defaultValue;
    }
}
