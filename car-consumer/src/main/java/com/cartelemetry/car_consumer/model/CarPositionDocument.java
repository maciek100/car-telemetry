package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "car_positions")
public class CarPositionDocument {
    @Id
    private String id;
    private String vin;
    private long timestamp;
    private double latitude;
    private double longitude;
    private double speed;
    private double engineTemp;
    private double gasTankLevel;
    private String obd2ErrorCode;
    private boolean processed = false;
}