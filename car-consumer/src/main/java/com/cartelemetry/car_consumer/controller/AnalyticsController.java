package com.cartelemetry.car_consumer.controller;

import com.cartelemetry.car_consumer.dto.*;
import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.car_consumer.repository.CompletedTripRepository;
import com.cartelemetry.car_consumer.repository.CurrentTripRepository;
import com.cartelemetry.car_consumer.repository.SpeedAlertRepository;
import com.cartelemetry.car_consumer.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/analytics")
@RequiredArgsConstructor
public class AnalyticsController {
    private final AnalyticsService analyticsService;
    private final CarPositionRepository carPositionRepository;
    private final CompletedTripRepository completedTripRepository;
    private final CurrentTripRepository currentTripRepository;
    private final SpeedAlertRepository speedAlertRepository;
    private final MongoTemplate mongoTemplate;

     // Trigger recomputation : POST /analytics/process
    @PostMapping("/process")
    public ResponseEntity<AnalyticsProcessResponse> triggerProcessing () {
        if (analyticsService.isRunning()) {
        //boolean triggered = analyticsService.processAnalytics();
        //if (!triggered) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                            .body(new AnalyticsProcessResponse(
                                    "ALREADY RUNNING",
                            "Analytics already executing.",
                                    analyticsService.getLastTriggeredAt()));
        }
        analyticsService.processAnalytics();
        return ResponseEntity.accepted()
                .body(new AnalyticsProcessResponse(
                        "ACCEPTED",
                        "Analytics triggered.",
                        analyticsService.getLastTriggeredAt()));
    }

    @GetMapping("/summary")
    public FleetSummaryDto getAnalyticsSummary () {
        return new FleetSummaryDto(
                mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class).size(),
                (int) currentTripRepository.count(),
                (int) completedTripRepository.count(),
                (int) speedAlertRepository.count(),
                analyticsService.getLastTriggeredAt()
        );
    }

    @GetMapping("/{vin}/summary")
    public ResponseEntity<?> getVehicleSummary(@PathVariable String vin) {
        if (!mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class)
                .contains(vin)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(new ErrorResponse(
                            "Vehicle with VIN: " + vin + " not found",
                            404,
                        System.currentTimeMillis()));
        }
        TripAggregationResult stats = completedTripRepository.getTripStatsByVin(vin);

        // handle no completed trips
        double totalDistance = stats != null ? stats.totalDistance() : 0.0;
        double avgSpeed = stats != null ? stats.avgSpeed() : 0.0;

        return ResponseEntity.ok(new VehicleSummaryDto(
                vin,
                currentTripRepository.findByVin(vin).orElse(null),
                (int) completedTripRepository.countByVin(vin),
                totalDistance,
                avgSpeed,
                (int) speedAlertRepository.countByVin(vin),
                analyticsService.getLastTriggeredAt()
        ));
    }
    /**
     * // Read results with timestamp
     * GET /analytics/summary
     * → fleet-wide: total vehicles, total distance, alerts count, etc.
     *
     * GET /analytics/{vin}/summary
     * → per vehicle: current trip, completed trips count,
     *    total distance, avg speed, alerts count, lastComputedAt
     */
}
