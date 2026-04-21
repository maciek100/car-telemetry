package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import com.google.protobuf.MapEntry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Service
public class CarDiagnosticsGenerator {

    VehicleRegistry vehicleRegistry;
    private final Random random = new Random();
    Map<String, Integer> odometers = new HashMap<>();
    public CarDiagnosticsGenerator(VehicleRegistry vehicleRegistry) {
        this.vehicleRegistry = vehicleRegistry;
    }

    @PostConstruct
    public void init () {
        vehicleRegistry.getVins()
                .forEach(vin ->
                        odometers.put(vin, 5000 + random.nextInt(100000)));
    }

    public CarDiagnostics generateDiagnostics() {
        String vin = vehicleRegistry.randomVin();
        int currentOdometer = odometers.get(vin) + random.nextInt(10) + 1;
        odometers.put(vin, currentOdometer);
         CarDiagnostics.Builder builder = CarDiagnostics.newBuilder()
                .setVin(vin)
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(180 + random.nextDouble() * 60)
                .setFuelLevel(random.nextDouble())
                .setBatteryVoltage(12.0 + random.nextDouble() * 2.4)
                .setOilPressure(25 * random.nextDouble() * 40) // 25 - 65 PSI
                .setRpm(700 + random.nextInt(3300)) //700 - 4000
                .setTirePressureFL(26.0 + random.nextDouble() * 5)
                .setTirePressureRL(26.0 + random.nextDouble() * 5)
                .setTirePressureFR(26.0 + random.nextDouble() * 5)
                .setTirePressureRR(25.9 + random.nextDouble() * 5)
                .setOdometer(currentOdometer);

        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0420");
        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0128");
        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0300");
        return builder.build();
    }

}
