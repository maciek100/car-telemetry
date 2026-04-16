package com.cartelemetry.car_consumer.dto;

// 202 Accepted - triggered successfully
// 409 Conflict - already running
// 500 Internal Server Error - something went wrong
public record AnalyticsProcessResponse(
        String status,        // ACCEPTED, FAILED, ALREADY_RUNNING
        String message,
        long triggeredAt
) {}
