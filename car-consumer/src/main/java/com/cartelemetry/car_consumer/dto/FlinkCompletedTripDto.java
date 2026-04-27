package com.cartelemetry.car_consumer.dto;

public record FlinkCompletedTripDto(
        String vin,
        long timestamp,
        long lastUpdateTimestamp,
        double totalDistanceMeters,
        double maxSpeedKph,
        double averageSpeedKph,
        int totalReadings,
        double startLat,
        double startLon,
        double lastLat,
        double lastLon) {}
