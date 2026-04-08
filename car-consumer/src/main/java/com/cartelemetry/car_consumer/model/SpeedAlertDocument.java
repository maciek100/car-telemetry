package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "speed_alerts")
public class SpeedAlertDocument {
    @Id
    private String id;
    private String vin;
    private long timestamp;
    private double latitude;
    private double longitude;
    private double computedSpeedKph;
    private double speedLimitKph;
}