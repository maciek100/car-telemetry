package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAmount;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class CarPositionGenerator {
    private static Logger log = LoggerFactory.getLogger(CarPositionGenerator.class);
    private static final Random random = new Random();

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

    private static final String[] VINS = {
            "1HGBH41JXMN109186",
            "2T1BURHE0JC043821",
            "3VWFE21C04M000001"
    };

    private static Map<String, VehicleLocation> vehicleStates = new HashMap<>();

    public CarPositionGenerator() {

            vehicleStates.put("1HGBH41JXMN109186",
                    new VehicleLocation(30.266, -97.730, random.nextDouble() * 360, false, null));
        vehicleStates.put("2T1BURHE0JC043821",
                new VehicleLocation(30.280, -97.750, random.nextDouble() * 360, false, null));
        vehicleStates.put("3VWFE21C04M000001",
                new VehicleLocation(30.250, -97.710, random.nextDouble() * 360, true, Instant.now()));
        }


    public CarPosition generate() {
        String vin = VINS[random.nextInt(VINS.length)];
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
        log.info("Vehicle {} STOPPED {}", vin, vehicleLocation.stopped);
        double newHeading = vehicleLocation.heading() + (random.nextDouble() - 0.5) * 20;
        double newLat = vehicleLocation.latitude() + Math.cos(Math.toRadians(newHeading)) * 0.001;
        double newLog = vehicleLocation.longitude() + Math.cos(Math.toRadians(newHeading)) * 0.001;
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
}
