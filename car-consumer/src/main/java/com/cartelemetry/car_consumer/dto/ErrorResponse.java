package com.cartelemetry.car_consumer.dto;

public record ErrorResponse(
        String message,
        int status,
        long timeStamp) {}
