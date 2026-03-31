package com.uberlite;

import com.uber.h3core.H3Core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Immutable record representing a driver GPS ping.
 * Uses Java 21 record semantics — no boilerplate.
 *
 * @param driverId   UUID string
 * @param lat        WGS84 latitude
 * @param lon        WGS84 longitude
 * @param timestampMs epoch millis
 * @param status     AVAILABLE | ON_TRIP | OFFLINE
 */
public record DriverLocationUpdate(
    String driverId,
    double lat,
    double lon,
    long   timestampMs,
    String status
) {
    private static final H3Core H3;
    static {
        try { H3 = H3Core.newInstance(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Composite key for H3CityPartitioner: "<driverId>|<lat>|<lon>" */
    public byte[] partitionKey() {
        return ("%s|%f|%f".formatted(driverId, lat, lon))
            .getBytes(StandardCharsets.UTF_8);
    }

    /** Value payload: compact CSV for this lesson (use Avro/Protobuf in prod) */
    public byte[] toBytes() {
        return ("%s,%f,%f,%d,%s".formatted(driverId, lat, lon, timestampMs, status))
            .getBytes(StandardCharsets.UTF_8);
    }

    /** H3 Resolution-9 index for fine-grained matching within the Streams topology */
    public long h3R9Index() {
        return H3.latLngToCell(lat, lon, 9);
    }

    /** H3 Resolution-4 index (city-level) used for partitioning */
    public long h3R4Index() {
        return H3.latLngToCell(lat, lon, 4);
    }
}
