package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@CompoundIndex(def = "{'vin': 1, 'timestamp': 1}", unique = true)
@Document(collection = "car_positions")
public class CarPositionDocument {
    @Id
    private String id;
    private String vin;
    private long timestamp;
    private double latitude;
    private double longitude;
    private double speed;
    private double heading;
    private boolean processed = false;
}