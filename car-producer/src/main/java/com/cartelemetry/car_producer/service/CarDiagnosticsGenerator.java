package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
// TODO: THIS CLASS IS DISABLED ... CURRENTLY NOT NEEDED ... at least in this iteration.
//   It will be "soon" reformatted to use VT model instead of "batches" ...
//@Service
public class CarDiagnosticsGenerator {

    VehicleRegistry vehicleRegistry;
    private final Random random = new Random();
    Map<String, Integer> odometers = new HashMap<>();
    public CarDiagnosticsGenerator(VehicleRegistry vehicleRegistry) {
        this.vehicleRegistry = vehicleRegistry;
    }

    static double engineTempMax = 240.0;
    static double engineTempMin = 180.0;
    static double fuelLevelMax = 1.0;
    static double fuelLevelMin = 0.0;
    static double batteryVoltageMax = 14.4;
    static double batteryVoltageMin = 12.0;
    static double tirePressureMax = 31.0;
    static double tirePressureMin = 26.0;
    static double oilPressureMax = 65.0;
    static double oilPressureMin = 25.0;
    static int rpmMin = 700;
    static int rpmMax = 4000;
    static int odometerMin = 5000;

    @PostConstruct
    public void init () {
        vehicleRegistry.getVins()
                .forEach(vin ->
                        odometers.put(vin, odometerMin + random.nextInt(100000)));
    }

    public CarDiagnostics generateDiagnostics() {
        String vin = vehicleRegistry.randomVin();
        int currentOdometer = odometers.get(vin) + random.nextInt(10) + 1;
        odometers.put(vin, currentOdometer);

         CarDiagnostics.Builder builder = CarDiagnostics.newBuilder()
                .setVin(vin)
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(engineTempMin + random.nextDouble() * 60)
                .setFuelLevel(random.nextDouble())
                .setBatteryVoltage(batteryVoltageMin + random.nextDouble() * 2.4)
                .setOilPressure(oilPressureMin + random.nextDouble() * 40) // 25 - 65 PSI
                 .setRpm(rpmMin + random.nextInt(rpmMax - rpmMin)) //700 - 4000
                .setTirePressureFL(tirePressureMin + random.nextDouble() * 5)
                .setTirePressureRL(tirePressureMin + random.nextDouble() * 5)
                .setTirePressureFR(tirePressureMin + random.nextDouble() * 5)
                .setTirePressureRR(tirePressureMin + random.nextDouble() * 5)
                .setOdometer(currentOdometer);

        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0420");
        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0128");
        if (random.nextInt(10) == 0) builder.addObd2ErrorCodes("P0300");
        return builder.build();
    }

}
