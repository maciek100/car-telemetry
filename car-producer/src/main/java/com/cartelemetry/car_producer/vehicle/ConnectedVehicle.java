package com.cartelemetry.car_producer.vehicle;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Instant;
import java.util.*;

public class ConnectedVehicle implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(ConnectedVehicle.class);
    //@Value("${kafka.topic.vehicle.positions}")
    private String topic = "vehicle-positions";
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final Random random = new Random();
    private final String vin;
    private VehicleLocation location;
    private final Deque<CarPosition> localPool;
    //private final KafkaTemplate kafka;
    //VehicleLocation location = initialLocation(vin);
    public ConnectedVehicle(KafkaTemplate<String, byte[]> kafkaTemplate, String vin) {
        this.kafkaTemplate = kafkaTemplate;
        this.vin = vin;
        this.location =  initialLocation(vin);
        this.localPool = new ArrayDeque<>(10);
    }

    @Override
    public void run() {
        while (true) {
            try {
            long timeStamp = System.currentTimeMillis();
            // 1. Generate position:
            CarPosition position = nextPosition(vin, location, timeStamp);
            updateLocation(location, position);

            // 2. Add to local pool:
            if (localPool.size() >= 10) {
                localPool.pollFirst(); // cull oldest ✓
            }
            localPool.offer(position);

            // 3. Pick random and send:
            List<CarPosition> snapshot =
                    new ArrayList<>(localPool);
            CarPosition toSend = snapshot
                    .get(random.nextInt(snapshot.size()));
            log.info("Sending vehicle {} at {}", toSend.getVin(), Instant.ofEpochMilli(toSend.getTimestamp()).toString());

            kafkaTemplate.send(topic, toSend.getVin(), toSend.toByteArray());

            log.info("VIN {} sent ts:{} pool:{}", vin, toSend.getTimestamp(), localPool.size());

            // 4. Wait before next generation:
            Thread.sleep(1000);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
    private VehicleLocation initialLocation(String vin) {
        return new VehicleLocation(
                vin,
                30.266 + random.nextDouble() * 0.1,
                -97.730 + random.nextDouble() * 0.1,
                random.nextDouble() * 360,
                (30 + random.nextDouble() * 50),
                false,
                null);
    }

    private CarPosition nextPosition(String vin, VehicleLocation vLocation, long tStamp) {
        //if (vLocation.stopped()) {
        //    if (Instant.now().isAfter(vLocation.stopUntil())) {
        //        log.info("vehicle {} resuming", vin);
        //    } else {
        //        log.info("Vehicle {} is stopped until {}", vin, vLocation.stopUntil);
        //        return null;
        //    }
        //}
        /*
        if (random.nextInt(500) < 1) {
            Instant stopUntil = Instant.now().plusSeconds(random.nextInt(300) + 300);
            vLocation.withStopped(stopUntil);
            log.info("Vehicle {} stopping until {}", vin, stopUntil);
            return null;
        }
         */
        if (vin == null) {
            log.info("LARUM vehicle vin is null");
        }
        //log.info("Vehicle {} is at {}", vin, vLocation);

        double delta = random.nextInt(100) < 5 ? 0.00020 : 0.00012;
        double newHeading = (vLocation.heading() + (random.nextDouble() - 0.5) * 20) % 360;
        if (newHeading < 0) newHeading += 360;
        double newLat = vLocation.latitude() + Math.cos(Math.toRadians(newHeading)) * delta;
        double newLog = vLocation.longitude() + Math.sin(Math.toRadians(newHeading)) * delta;
        double newSpeed = computeNewSpeed(vLocation.currentSpeed());
        return CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(tStamp)
                .setSpeed(newSpeed)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(newLat)
                        .setLongitude(newLog)
                        .build())
                .setHeading(newHeading)
                .build();

    }

    double computeNewSpeed(double currentSpeed) {
        double variation = (random.nextDouble() - 0.5) * 10;
        double newSpeed = currentSpeed + variation;
        return Math.max(20, Math.min(110, newSpeed));
    }

    private void updateLocation(VehicleLocation vLocation, CarPosition newPosition) {
        vLocation.withNewPosition(
                newPosition.getVin(),
                newPosition.getLocation().getLatitude(),
                newPosition.getLocation().getLongitude(),
                newPosition.getHeading(),
                newPosition.getSpeed());

    }
}