package com.cartelemetry.car_producer.service;

import com.cartelemetry.car_producer.vehicle.ConnectedVehicle;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VehiclePositionGenerator {
    private static final Logger log = LoggerFactory.getLogger(VehiclePositionGenerator.class);
    private static List<String> vinList;
    private final VehicleRegistry vehicleRegistry;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public VehiclePositionGenerator(VehicleRegistry vehicleRegistry, KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.vehicleRegistry = vehicleRegistry;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostConstruct
    public void init() {
        vinList = vehicleRegistry.getVins();
        vinList.forEach(vin -> {
            Thread.ofVirtual()
                    .name("vehicle-"+ vin)
                    .start(new ConnectedVehicle(kafkaTemplate, vin));
        });
    }
}
