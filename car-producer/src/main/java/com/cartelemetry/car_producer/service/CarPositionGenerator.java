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
            double currentSpeed,
            boolean stopped,
            Instant stopUntil) {
        VehicleLocation withNewPosition(double newLat, double newLon, double newHeading, double newSpeed) {
            return new VehicleLocation(newLat, newLon, newHeading, newSpeed, false, null);
        }
        VehicleLocation withStopped(Instant stopUntil) {
            return new VehicleLocation(this.latitude, this.longitude, this.heading, this.currentSpeed, true, stopUntil);
        }
        VehicleLocation withMoving() {
            return new VehicleLocation(this.latitude, this.longitude, this.heading, this.currentSpeed, false, null);
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
                    Instant.now().plusSeconds(random.nextInt(20) + 10) : null;
            vehicleStates.put(vin, new VehicleLocation(
                    30.266 + random.nextDouble() * 0.1,
                    -97.730 + random.nextDouble() * 0.1,
                            random.nextDouble() * 360,
                    (30 + random.nextDouble() * 50),
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
        if (random.nextInt(500) < 1) {
            Instant stopUntil = Instant.now().plusSeconds(random.nextInt(300) + 300);
            vehicleStates.put(vin, vehicleLocation.withStopped(stopUntil));
            log.info("Vehicle {} stopping until {}", vin, stopUntil);
            return null;
        }
        log.info("Vehicle {} is at {}", vin, vehicleLocation);

        double delta = random.nextInt(100) < 5 ? 0.00020 : 0.00012;
        double newHeading = (vehicleLocation.heading() + (random.nextDouble() - 0.5) * 20) % 360;
        if (newHeading < 0) newHeading += 360;
        double newLat = vehicleLocation.latitude() + Math.cos(Math.toRadians(newHeading)) * delta;
        double newLog = vehicleLocation.longitude() + Math.sin(Math.toRadians(newHeading)) * delta;
        double newSpeed = computeNewSpeed(vehicleLocation.currentSpeed());
        vehicleStates.put(vin, vehicleLocation.withNewPosition(newLat, newLog, newHeading, newSpeed));
        //double speedMs = delta * 111111.0; // meters per second
        //double speedKph = speedMs * 3.6;   // convert to kph
        //speedKph = 177.7; //just a foul value ...
        return CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(batchTimestamp)
                .setSpeed(newSpeed)
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

    // package private ... for testing
    double computeNewSpeed(double currentSpeed) {
        double variation = (random.nextDouble() - 0.5) * 10;
        double newSpeed = currentSpeed + variation;
        return Math.max(20, Math.min(110, newSpeed));
    }

    /*

    //FAST & FURIOUS variant for ... later.
    private double computeNewSpeed(double currentSpeed) {
    // Occasional speeder!
    if (random.nextInt(100) < 5) {
        return computeSpeedingSpeed();  // ← just add this
    }

    double variation = (random.nextDouble() - 0.5) * 10;
    double newSpeed = currentSpeed + variation;
    return Math.max(20, Math.min(110, newSpeed));
}

private double computeSpeedingSpeed() {
    return 120 + random.nextDouble() * 40; // 120-160 kph
}
     */
}
