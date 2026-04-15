package com.cartelemetry.car_consumer.controller;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.model.CompletedTripDocument;
import com.cartelemetry.car_consumer.model.CurrentTripDocument;
import com.cartelemetry.car_consumer.model.SpeedAlertDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.car_consumer.repository.CompletedTripRepository;
import com.cartelemetry.car_consumer.repository.CurrentTripRepository;
import com.cartelemetry.car_consumer.repository.SpeedAlertRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.print.attribute.standard.PageRanges;
import java.util.List;

@RestController
@RequestMapping("/vehicles")
@RequiredArgsConstructor
public class VehicleController {
    private final CarPositionRepository carPositionRepository;
    private final CompletedTripRepository completedTripRepository;
    private final CurrentTripRepository currentTripRepository;
    private final SpeedAlertRepository speedAlertRepository;
    private final MongoTemplate mongoTemplate;

    // inject repositories you need

    // GET /vehicles
    @GetMapping()
    public List<String> getVehicles () {
        return mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class);
    }

    // GET /vehicles/{vin}/trip/current
    @GetMapping("/{vin}/trip/current")
    public ResponseEntity<CurrentTripDocument> getCurrentTrip(@PathVariable String vin) {
        return currentTripRepository.findByVin(vin)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // GET /vehicles/{vin}/trips
    @GetMapping("/{vin}/trips")
    public List<CompletedTripDocument> getVehicleTrips (@PathVariable String vin) {
        return completedTripRepository.findByVinOrderByTripStartTimestampDesc(vin);
    }

    // GET /vehicles/{vin}/alerts
    @GetMapping("/{vin}/alerts")
    public List<SpeedAlertDocument> getSpeedAlerts(@PathVariable String vin) {
        return speedAlertRepository.findByVinOrderByTimestampDesc(vin);
    }


    // GET /vehicles/{vin}/positions
    @GetMapping("/{vin}/positions")
    public List<CarPositionDocument> getVehiclePositions(
            @PathVariable String vin,
            @RequestParam(defaultValue = "100") int limit) {
        return carPositionRepository.findByVinOrderByTimestampDesc(
                vin, PageRequest.of(0, limit));
    }
}