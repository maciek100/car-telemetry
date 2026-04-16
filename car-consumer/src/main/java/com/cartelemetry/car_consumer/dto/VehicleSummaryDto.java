package com.cartelemetry.car_consumer.dto;

import com.cartelemetry.car_consumer.model.CurrentTripDocument;

public record VehicleSummaryDto(
   String vin,
   CurrentTripDocument currentTrip,
   int completedTripsCount,
   double totalDistanceMeters,
   double averageSpeedKph,
   int speedAlertsCount,
   long lastComputedAt) {}
