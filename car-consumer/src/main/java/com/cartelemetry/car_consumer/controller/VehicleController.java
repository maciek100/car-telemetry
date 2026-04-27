package com.cartelemetry.car_consumer.controller;

import com.cartelemetry.car_consumer.dto.ErrorResponse;
import com.cartelemetry.car_consumer.dto.FlinkCompletedTripDto;
import com.cartelemetry.car_consumer.dto.FlinkSpeedAlertDto;
import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.mongodb.client.FindIterable;
import lombok.RequiredArgsConstructor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/vehicles")
@RequiredArgsConstructor
public class VehicleController {

    private final CarPositionRepository carPositionRepository;
    private final MongoTemplate mongoTemplate;

    @GetMapping
    public List<String> getVehicles() {
        return mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class);
    }

    @GetMapping("/{vin}/trip/current")
    public ResponseEntity<?> getCurrentTrip(@PathVariable String vin) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(new ErrorResponse(
                        "Current trip state is managed by Flink in real-time",
                        503,
                        System.currentTimeMillis()));
    }

    @GetMapping("/{vin}/trips")
    public List<FlinkCompletedTripDto> getCompletedTrips(@PathVariable String vin) {
        // query flink_completed_trips
        return mongoTemplate.getCollection("flink_completed_trips")
                .find(new Document("vin", vin))
                .map(doc -> new FlinkCompletedTripDto(
                        doc.getString("vin"),
                        doc.getLong("timestamp"),
                        doc.getLong("lastUpdateTimestamp"),
                        doc.getDouble("totalDistanceMeters"),
                        doc.getDouble("maxSpeedKph"),
                        doc.getDouble("averageSpeedKph"),
                        doc.getInteger("totalReadings"),
                        doc.getDouble("startLat"),
                        doc.getDouble("startLon"),
                        doc.getDouble("lastLat"),
                        doc.getDouble("lastLon")))
                .into(new ArrayList<>());
    }

    @GetMapping("/{vin}/alerts")
    public List<FlinkSpeedAlertDto> getAlerts(@PathVariable String vin) {
        return mongoTemplate.getCollection("flink_speed_alerts")
                .find(new Document("vin", vin))
                .map(doc -> new FlinkSpeedAlertDto(
                        doc.getString("vin"),
                        doc.getLong("timestamp"),
                        doc.getDouble("computedSpeedKph"),
                        doc.getDouble("latitude"),
                        doc.getDouble("longitude")))
                .into(new ArrayList<>());
    }

    @GetMapping("/{vin}/positions")
    public List<CarPositionDocument> getPositions(
            @PathVariable String vin,
            @RequestParam(defaultValue = "100") int limit) {
        return carPositionRepository.findByVinOrderByTimestampDesc(
                vin, PageRequest.of(0, limit)
        );
    }
}
