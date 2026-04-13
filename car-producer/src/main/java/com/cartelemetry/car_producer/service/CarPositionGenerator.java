package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class CarPositionGenerator {
    private static Logger log = LoggerFactory.getLogger(CarPositionGenerator.class);
    private static final Random random = new Random();
    @Value("${generator.vehicle.count:3}")
    private int vehicleCount;

    private record VehicleLocation(
            double latitude,
            double longitude,
            double heading,
            boolean stopped,
            Instant stopUntil) {
        VehicleLocation withNewPosition(double newLat, double newLon, double newHeading) {
            return new VehicleLocation(newLat, newLon, newHeading, false, null);
        }
        VehicleLocation withStopped(Instant stopUntil) {
            return new VehicleLocation(this.latitude, this.longitude, this.heading, true, stopUntil);
        }
        VehicleLocation withMoving() {
            return new VehicleLocation(this.latitude, this.longitude, this.heading, false, null);
        }
    }

    private static final Map<String, VehicleLocation> vehicleStates = new HashMap<>();
    private static List<String> vinList;

    public CarPositionGenerator() {}

    @PostConstruct
    public void init () {
        for (int i = 0; i < vehicleCount; i++) {
            String vin = makeVin(i);
            vehicleStates.put(vin, new VehicleLocation(
                    30.266 + random.nextDouble() * 0.1,
                    -97.730 + random.nextDouble() * 0.1,
                            random.nextDouble() * 360,
                    false,
                    null));
        }
        vinList = vehicleStates.keySet().stream().toList();
    }

    private static String makeVin(int index) {
        return String.format("VIN%06d", index);
    }

    private CarPosition generate(String vin) {
        VehicleLocation vehicleLocation = vehicleStates.get(vin);
        if (vehicleLocation.stopped()) {
            log.info("Vehicle {} is stopped until {}", vin, vehicleLocation.stopUntil);
            return null;
        }
        if (random.nextInt(100) < 2) {
            Instant stopUntil = Instant.now().plusSeconds(random.nextInt(300) + 300);
            vehicleStates.put(vin, vehicleLocation.withStopped(stopUntil));
            log.info("Vehicle {} stopping until {}", vin, stopUntil);
        }
        log.info("Vehicle ABC {}", vehicleLocation);
        if (vehicleLocation.stopped)
            log.info("Vehicle {} STOPPED until {}", vin, vehicleLocation.stopUntil);
        double newHeading = vehicleLocation.heading() + (random.nextDouble() - 0.5) * 20;
        double newLat = vehicleLocation.latitude() + Math.cos(Math.toRadians(newHeading)) * 0.00015;
        double newLog = vehicleLocation.longitude() + Math.sin(Math.toRadians(newHeading)) * 0.00015;
        vehicleStates.put(vin, vehicleLocation.withNewPosition(newLat, newLog, newHeading));
        return CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(Instant.now().toEpochMilli())
                .setEngineTemp(180 + random.nextDouble() * 40)
                .setSpeed(random.nextDouble() * 120)
                .setGasTankLevel(random.nextDouble())
                .setObd2ErrorCode(random.nextInt(10) == 0 ? "P0420" : "")
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(newLat)
                        .setLongitude(newLog)
                        .build())
                .build();
    }

    public List<CarPosition> generateAll () {
        return vinList.stream()
                .map(this::generate)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
