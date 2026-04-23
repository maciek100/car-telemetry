package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CarPositionGenerator {
    private static final Logger log = LoggerFactory.getLogger(CarPositionGenerator.class);
    private static final Random random = new Random();
    private final VehicleRegistry vehicleRegistry;

    public CarPositionGenerator(VehicleRegistry vehicleRegistry) {
        this.vehicleRegistry = vehicleRegistry;
    }
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



    @PostConstruct
    public void init () {

        vinList = vehicleRegistry.getVins();
        for (String vin : vinList) {
            boolean initialStop = random.nextBoolean();
            Instant stopUntil = initialStop ?
                    Instant.now().plusSeconds(random.nextInt(180)) : null;
            vehicleStates.put(vin, new VehicleLocation(
                    30.266 + random.nextDouble() * 0.1,
                    -97.730 + random.nextDouble() * 0.1,
                            random.nextDouble() * 360,
                    initialStop,
                    stopUntil));
        }
    }

    private static String makeVin(int index) {
        return String.format("VIN%06d", index);
    }

    private CarPosition generatePosition(String vin, long batchTimestamp) {
        VehicleLocation vehicleLocation = vehicleStates.get(vin);
        if (vehicleLocation.stopped()) {
            if (Instant.now().isAfter(vehicleLocation.stopUntil())) {
                log.info("vehicle {} resuming", vin);
                vehicleStates.put(vin, vehicleLocation.withMoving());
            } else {
                log.info("Vehicle {} is stopped until {}", vin, vehicleLocation.stopUntil);
                return null;
            }
        }
        if (random.nextInt(200) < 1) {
            Instant stopUntil = Instant.now().plusSeconds(random.nextInt(300) + 300);
            vehicleStates.put(vin, vehicleLocation.withStopped(stopUntil));
            log.info("Vehicle {} stopping until {}", vin, stopUntil);
            return null;
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
                .setTimestamp(batchTimestamp)
                .setSpeed(random.nextDouble() * 120)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(newLat)
                        .setLongitude(newLog)
                        .build())
                .setHeading(newHeading)
                .build();
    }

    public List<CarPosition> generateAll (long batchTimestamp) {
        return vinList.stream()
                .map(vin -> generatePosition(vin, batchTimestamp))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }
}
