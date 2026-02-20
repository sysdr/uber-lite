package com.uberlite.lesson27;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class H3UtilTest {
    @Test
    void cellOfReturnsNonZero() {
        long cell = H3Util.cellOf(41.85, -87.65);
        assertTrue(cell != 0);
    }
    @Test
    void partitionForIsDeterministic() {
        long cell = H3Util.cellOf(40.71, -74.00);
        int p = H3Util.partitionFor(cell, 12);
        assertTrue(p >= 0 && p < 12);
        assertEquals(p, H3Util.partitionFor(cell, 12));
    }
}
