package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.util.AssertionErrors.assertTrue;

public class CarDiagnosticsGeneratorTest {

    CarDiagnosticsGenerator generator;
   private static final String VIN = "VIN000001";
    @BeforeEach
    public void setUp() {
        VehicleRegistry registry = mock(VehicleRegistry.class);
        when(registry.randomVin()).thenReturn(VIN);
        when(registry.getVins()).thenReturn(List.of(VIN));
        generator = new CarDiagnosticsGenerator(registry);
        generator.init();
    }

    @Test
    public void generateGarDiagnosticsTest() {
        for (int i = 0; i < 100; i++) {
            CarDiagnostics cd1 = generator.generateDiagnostics();
            assertTrue("Engine temperature too high : " + cd1.getEngineTemp(), cd1.getEngineTemp() <= CarDiagnosticsGenerator.engineTempMax);
            assertTrue("Engine temperature too low : " + cd1.getEngineTemp(), cd1.getEngineTemp() >= CarDiagnosticsGenerator.engineTempMin);

            assertTrue("Fuel level too high : " + cd1.getFuelLevel(), cd1.getFuelLevel() <= CarDiagnosticsGenerator.fuelLevelMax);
            assertTrue("Fuel level too low : " + cd1.getFuelLevel(), cd1.getFuelLevel() >= CarDiagnosticsGenerator.fuelLevelMin);

            assertTrue("Battery voltage too high : " + cd1.getBatteryVoltage(), cd1.getBatteryVoltage() <= CarDiagnosticsGenerator.batteryVoltageMax);
            assertTrue("Battery voltage too low : " + cd1.getBatteryVoltage(), cd1.getBatteryVoltage() >= CarDiagnosticsGenerator.batteryVoltageMin);

            assertTrue("Tire pressure too high : " + cd1.getTirePressureFL(), cd1.getTirePressureFL() <= CarDiagnosticsGenerator.tirePressureMax);
            assertTrue("Tire pressure too low : " + cd1.getTirePressureFL(), cd1.getTirePressureFL() >= CarDiagnosticsGenerator.tirePressureMin);

            assertTrue("Oil pressure too high : " + cd1.getOilPressure(), cd1.getOilPressure() <= CarDiagnosticsGenerator.oilPressureMax);
            assertTrue("Oil pressure too low : " + cd1.getOilPressure(), cd1.getOilPressure() >= CarDiagnosticsGenerator.oilPressureMin);
            assertTrue("RPM too high: " + cd1.getRpm(), cd1.getRpm() <= CarDiagnosticsGenerator.rpmMax);
            assertTrue("RPM too low: " + cd1.getRpm(), cd1.getRpm() >= CarDiagnosticsGenerator.rpmMin);
            assertTrue("Vin mismatch ", VIN.equals(cd1.getVin()));
            CarDiagnostics cd2 = generator.generateDiagnostics();
            assertTrue("Odometer should NEVER go back", cd2.getOdometer() > cd1.getOdometer());
        }

    }
}
