package com.cartelemetry.car_producer.service;

//import jakarta.annotation.PostConstruct;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class VehicleRegistry {
    private static final Logger log = LoggerFactory.getLogger(VehicleRegistry.class);
    private final List<String> vinList;

    public VehicleRegistry(@Value("${generator.vehicle.count}") int vehicleCount) {
        vinList = IntStream.range(0, vehicleCount)
                .mapToObj(i -> String.format("VIN%06d", i))
                .collect(Collectors.toList());
    }

    @PostConstruct
    public void init() {
        // or in constructor
        log.info("VehicleRegistry initialized with {} VINs: {}", vinList.size(), vinList);
    }

    public List<String> getVins() {
        return Collections.unmodifiableList(vinList);
    }

    public String randomVin() {
        return vinList.get(new Random().nextInt(vinList.size()));
    }
}
