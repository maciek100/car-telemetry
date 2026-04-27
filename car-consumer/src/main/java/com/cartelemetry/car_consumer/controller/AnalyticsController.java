package com.cartelemetry.car_consumer.controller;

import com.cartelemetry.car_consumer.dto.AnalyticsProcessResponse;
import com.cartelemetry.car_consumer.dto.ErrorResponse;
import com.cartelemetry.car_consumer.dto.FleetSummaryDto;
import com.cartelemetry.car_consumer.dto.VehicleSummaryDto;
import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/analytics")
@RequiredArgsConstructor
public class AnalyticsController {
    private final CarPositionRepository carPositionRepository;
    private final MongoTemplate mongoTemplate;

     // Trigger recomputation : POST /analytics/process
    @PostMapping("/process")
    public ResponseEntity<AnalyticsProcessResponse> triggerProcessing () {
        return ResponseEntity.ok(
                new AnalyticsProcessResponse(
                "FLINK_ACTIVE",
                "Analytics is handled by Flink in real-time. No manual trigger needed!",
                System.currentTimeMillis()));
    }

    @GetMapping("/summary")
    public FleetSummaryDto getAnalyticsSummary () {
        return new FleetSummaryDto(
                mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class).size(),
                (int) mongoTemplate.getCollection("flink_completed_trips").countDocuments(),
                (int) mongoTemplate.getCollection("flink_speed_alerts").countDocuments(),
                System.currentTimeMillis());
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
        List<Document> aggResult = mongoTemplate.getCollection("flink_completed_trips")
                .aggregate(List.of(
                        new Document("$match", new Document("vin", vin)),
                        new Document("$group", new Document("_id", "$vin")
                                .append("totalDistance", new Document("$sum", "$totalDistanceMeters"))
                                .append("avgSpeed", new Document("$avg", "$averageSpeedKph")))
                ))
                .into(new ArrayList<>());
        double totalDistanceKm = aggResult.isEmpty() ? 0.0 :
                Math.round(aggResult.get(0).getDouble("totalDistance") / 10.0) / 100.0;
        double avgSpeedKph = aggResult.isEmpty() ? 0.0 :
                Math.round(aggResult.get(0).getDouble("avgSpeed") * 100.0) / 100.0;


        long completedTrips = mongoTemplate.getCollection("flink_completed_trips")
                .countDocuments(new Document("vin", vin));
        long speedAlerts = mongoTemplate.getCollection("flink_speed_alerts")
                .countDocuments(new Document("vin", vin));

        return ResponseEntity.ok(new VehicleSummaryDto(
                vin,
                (int) completedTrips,
                totalDistanceKm,
                avgSpeedKph,
                (int) speedAlerts,
                System.currentTimeMillis()));
    }
}
