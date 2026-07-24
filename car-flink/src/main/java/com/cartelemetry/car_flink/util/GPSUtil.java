package com.cartelemetry.car_flink.util;

public class GPSUtil {

    private GPSUtil() {
        throw new UnsupportedOperationException(
                "GPSUtil is a utility class");
    }

    private static final double EARTH_RADIUS = 6371000; // meters

    /**
     * Calculates distance between two GPS coordinates
     * using Haversine formula
     * Returns distance in meters
     */
    public static double haversine(double lat1, double lon1,
                                   double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) *
                        Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon/2) * Math.sin(dLon/2);
        return EARTH_RADIUS * 2 *
                Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }

    /**
     * Computes speed in kph from distance and time
     */
    public static double computeSpeedKph(double distanceMeters,
                                         long fromTs, long toTs) {
        double seconds = (toTs - fromTs) / 1000.0;
        if (seconds <= 0) return 0;
        return (distanceMeters / seconds) * 3.6;
    }

    /**
     * Convenience method — computes speed directly from coordinates
     */
    public static double computeSpeedKph(double lat1, double lon1,
                                         double lat2, double lon2,
                                         long fromTs, long toTs) {
        double distance = haversine(lat1, lon1, lat2, lon2);
        return computeSpeedKph(distance, fromTs, toTs);
    }

    /**
     * Formats duration in milliseconds to human-readable string
     */
    public static String formatDuration(long millis) {
        long minutes = millis / 60000;
        long seconds = (millis % 60000) / 1000;
        return String.format("%dm%02ds", minutes, seconds);
    }
}
