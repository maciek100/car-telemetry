package com.cartelemetry.car_consumer.dto;

public record FlinkSpeedAlertDto(
        String vin,
        long timestamp,
        double computedSpeedKph,
        double latitude,
        double longitude
) {}
