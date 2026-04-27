package com.cartelemetry.car_consumer.dto;

public record FleetSummaryDto(
        int totalVehicles,
        int completedTrips,
        int totalSpeedAlerts,
        long lastComputedAt
) {}