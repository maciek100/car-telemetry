package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "current_trips")
public class CurrentTripDocument {
    @Id
    private String id;
    private String vin;
    private long tripStartTimestamp;
    private long lastUpdateTimestamp;
    private double startLat;
    private double startLon;
    private double lastLat;
    private double lastLon;
    private double totalDistanceMeters;
    private double averageSpeedKph;
    private double maxSpeedKph;
    private int totalReadings;
}