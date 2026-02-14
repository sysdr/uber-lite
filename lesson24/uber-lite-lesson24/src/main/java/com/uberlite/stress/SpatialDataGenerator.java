package com.uberlite.stress;

import com.uber.h3core.H3Core;
import com.uber.h3core.util.LatLng;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Deterministic H3-based spatial data generator.
 *
 * Simulates realistic ride-hailing hotspot distribution:
 * - 5 hotspot zones (airports, downtown cores) at H3 Resolution 6
 * - Each hotspot fills k=4 ring at Resolution 9 (~174m² cells)
 * - Driver positions sampled from these cell centroids + jitter
 *
 * This produces realistic partition skew (hotspot cells → same partitions)
 * without randomized uniform distribution that would hide bottlenecks.
 */
public class SpatialDataGenerator {

    private static final int HOTSPOT_RES   = 6;
    private static final int DRIVER_RES    = 9;
    private static final int KRING_RADIUS  = 4;

    // San Francisco Bay Area hotspots at H3 Resolution 6
    private static final double[][] HOTSPOT_COORDS = {
        {37.6213, -122.3790},  // SFO Airport
        {37.7749, -122.4194},  // Downtown SF
        {37.3382, -121.8863},  // San Jose Downtown
        {37.8716, -122.2727},  // UC Berkeley
        {37.4419, -122.1430},  // Palo Alto
    };

    private final H3Core h3;
    private final List<Long> driverCells;

    public SpatialDataGenerator() throws IOException {
        this.h3 = H3Core.newInstance();
        this.driverCells = new ArrayList<>();
        buildSpatialIndex();
    }

    private void buildSpatialIndex() {
        for (var coords : HOTSPOT_COORDS) {
            long hotspotCell = h3.latLngToCell(coords[0], coords[1], HOTSPOT_RES);
            // Get all Resolution 9 children within k=4 ring at resolution 6,
            // then sample their centroids
            List<Long> kring = h3.gridDisk(hotspotCell, KRING_RADIUS);
            for (long cell6 : kring) {
                // Get child cells at resolution 9
                List<Long> children = h3.cellToChildren(cell6, DRIVER_RES);
                // Take 20% sample to keep list manageable but still hotspot-skewed
                int sampleSize = Math.max(1, children.size() / 5);
                for (int i = 0; i < sampleSize; i++) {
                    driverCells.add(children.get(i));
                }
            }
        }
        System.out.printf("==> SpatialDataGenerator: %,d driver cells across %d hotspots%n",
                driverCells.size(), HOTSPOT_COORDS.length);
    }

    /**
     * Generate a DriverLocationEvent with realistic H3-based position.
     * Key is H3 cell index string (routes to same partition for same cell).
     */
    public DriverLocationEvent nextEvent() {
        var rng = ThreadLocalRandom.current();
        long cellId = driverCells.get(rng.nextInt(driverCells.size()));
        LatLng centroid = h3.cellToLatLng(cellId);

        // Sub-cell jitter: ±0.0005 degrees (~55m) to simulate real GPS noise
        double lat = centroid.lat + (rng.nextDouble() - 0.5) * 0.001;
        double lng = centroid.lng + (rng.nextDouble() - 0.5) * 0.001;

        return new DriverLocationEvent(
                "driver-" + rng.nextInt(100_000),
                lat,
                lng,
                h3.h3ToString(cellId),
                DRIVER_RES,
                System.currentTimeMillis()
        );
    }

    public int getCellCount() {
        return driverCells.size();
    }
}
