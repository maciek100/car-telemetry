package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "completed_trips")
public class CompletedTripDocument {
    @Id
    private String id;
    private String vin;
    private long tripStartTimestamp;
    private long tripEndTimestamp;
    private double startLat;
    private double startLon;
    private double endLat;
    private double endLon;
    private double totalDistanceMeters;
    private double averageSpeedKph;
    private double maxSpeedKph;
    private int totalReadings;
    private long durationMinutes;
}