package com.uberlite;

import com.uber.h3core.H3Core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Immutable record representing a ride request from a rider.
 *
 * @param riderId     UUID string
 * @param pickupLat   WGS84 pickup latitude
 * @param pickupLon   WGS84 pickup longitude
 * @param timestampMs epoch millis
 */
public record RiderRequest(
    String riderId,
    double pickupLat,
    double pickupLon,
    long   timestampMs
) {
    private static final H3Core H3;
    static {
        try { H3 = H3Core.newInstance(); }
        catch (IOException e) { throw new UncheckedIOException(e); }
    }

    /** Composite key for H3CityPartitioner: "<riderId>|<lat>|<lon>" */
    public byte[] partitionKey() {
        return ("%s|%f|%f".formatted(riderId, pickupLat, pickupLon))
            .getBytes(StandardCharsets.UTF_8);
    }

    public byte[] toBytes() {
        return ("%s,%f,%f,%d".formatted(riderId, pickupLat, pickupLon, timestampMs))
            .getBytes(StandardCharsets.UTF_8);
    }

    public long h3R9Index() {
        return H3.latLngToCell(pickupLat, pickupLon, 9);
    }

    public long h3R4Index() {
        return H3.latLngToCell(pickupLat, pickupLon, 4);
    }
}
