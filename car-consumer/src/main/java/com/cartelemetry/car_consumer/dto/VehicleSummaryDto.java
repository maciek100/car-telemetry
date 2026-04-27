package com.cartelemetry.car_consumer.dto;

//import com.cartelemetry.car_consumer.model.CurrentTripDocument;

public record VehicleSummaryDto(
   String vin,
   int completedTripsCount,
   double totalDistanceKm,
   double averageSpeedKph,
   int speedAlertsCount,
   long lastComputedAt) {}
