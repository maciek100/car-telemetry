package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CarDiagnosticsGeneratorTest {
    @Mock
    VehicleRegistry vehicleRegistry;

    @InjectMocks
    CarDiagnosticsGenerator carDiagnosticsGenerator;

    @Test
    void odometerIncreasePerVin() {
        // GIVEN
        when(vehicleRegistry.randomVin()).thenReturn("VIN000001");
        when(vehicleRegistry.getVins()).thenReturn(List.of("VIN000001"));

        carDiagnosticsGenerator.init();  // initialize odometers

        // WHEN
        CarDiagnostics first = carDiagnosticsGenerator.generateDiagnostics();
        CarDiagnostics second = carDiagnosticsGenerator.generateDiagnostics();
        CarDiagnostics third = carDiagnosticsGenerator.generateDiagnostics();

        // THEN
        assertTrue(second.getOdometer() > first.getOdometer());
        assertTrue(third.getOdometer() > second.getOdometer());
        assertEquals("VIN000001", first.getVin());   // verify VIN matches odometer!
        assertEquals("VIN000001", second.getVin());
        assertEquals("VIN000001", third.getVin());
    }
}
