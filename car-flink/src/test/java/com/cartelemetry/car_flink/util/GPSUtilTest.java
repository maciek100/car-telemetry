package com.cartelemetry.car_flink.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.cartelemetry.car_flink.util.GPSUtil;
// No mocking needed — pure functions!
class GPSUtilTest {

    @Test
    void testHaversineAustin() {
        // Known distance: UT Tower to Capitol
        // approximately 1.2 km
        double distance = GPSUtil.haversine(
                30.2849, -97.7341,  // UT Tower
                30.2747, -97.7404); // Capitol

        // Should be ~1200 meters
        assertTrue(distance > 1100 && distance < 1300,
                "Expected ~1200m but got: " + distance);
    }

    @Test
    void testHaversineSamePoint() {
        double distance = GPSUtil.haversine(
                30.2672, -97.7431,
                30.2672, -97.7431);
        assertEquals(0.0, distance, 0.001);
    }

    @Test
    void testComputeSpeedKph() {
        // 100 meters in 10 seconds = 36 kph
        double speed = GPSUtil.computeSpeedKph(
                100.0, 0L, 10000L);
        assertEquals(36.0, speed, 0.001);
    }

    @Test
    void testComputeSpeedKphZeroTime() {
        // Same timestamp → should return 0
        double speed = GPSUtil.computeSpeedKph(
                100.0, 1000L, 1000L);
        assertEquals(0.0, speed, 0.001);
    }

    @Test
    void testFormatDuration() {
        assertEquals("1m30s",
                GPSUtil.formatDuration(90000));
        assertEquals("0m05s",
                GPSUtil.formatDuration(5000));
        assertEquals("10m00s",
                GPSUtil.formatDuration(600000));
    }

    @Test
    void testComputeSpeedFromCoordinates() {
        // Two points 13.8 meters apart, 1 second = 50 kph
        double lat1 = 30.2672;
        double lon1 = -97.7431;
        double lat2 = lat1 + 0.000124; // ~13.8 meters north
        double lon2 = lon1;

        double speed = GPSUtil.computeSpeedKph(
                lat1, lon1, lat2, lon2,
                0L, 1000L);

        // Should be approximately 50 kph
        assertTrue(speed > 45 && speed < 55,
                "Expected ~50 kph but got: " + speed);
    }
}
