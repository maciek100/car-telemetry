package com.cartelemetry.car_consumer.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;

@Data
@CompoundIndex(def = "{'vin': 1, 'timestamp': 1}", unique = true)
@Document(collection = "car_diagnostics")
public class CarDiagnosticsDocument {
    @Id
    private String id;
    private String vin;
    private long timestamp;
    private double engineTemp;
    private double gasTankLevel;
    private List<String> obd2ErrorCode;
    private double batteryVoltage;
    private double oilPressure;
    private int rpm;
    private int odometer;
    private double tirePressureFL;
    private double tirePressureFR;
    private double tirePressureRL;
    private double tirePressureRR;
}
