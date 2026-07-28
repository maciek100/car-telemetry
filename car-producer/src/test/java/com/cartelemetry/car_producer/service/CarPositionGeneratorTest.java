package com.cartelemetry.car_producer.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.cartelemetry.car_producer.service.CarPositionGenerator;

class CarPositionGeneratorTest {

    private CarPositionGenerator generator;

    @BeforeEach
    void setUp() {
        VehicleRegistry registry = mock(VehicleRegistry.class);
        when(registry.getVins())
                .thenReturn(List.of("VIN000001"));
        generator = new CarPositionGenerator(registry);
        generator.init(); // initialize vehicle states
    }

    @Test
    void testComputeNewSpeedStaysInRange() {
        for (int i = 0; i < 1000; i++) {
            double speed = generator.computeNewSpeed(60.0);
            assertTrue(speed >= 20.0 && speed <= 110.0,
                    "Speed out of range: " + speed);
        }
    }

    @Test
    void testComputeNewSpeedChangesGradually() {
        double currentSpeed = 60.0;
        for (int i = 0; i < 100; i++) {
            double newSpeed =
                    generator.computeNewSpeed(currentSpeed);
            assertTrue(
                    Math.abs(newSpeed - currentSpeed) <= 5.0,
                    "Speed changed too much!");
            currentSpeed = newSpeed;
        }
    }

    @Test
    void testComputeNewSpeedClampsAtMinimum() {
        for (int i = 0; i < 100; i++) {
            double speed = generator.computeNewSpeed(20.0);
            assertTrue(speed >= 20.0);
        }
    }

    @Test
    void testComputeNewSpeedClampsAtMaximum() {
        for (int i = 0; i < 100; i++) {
            double speed = generator.computeNewSpeed(110.0);
            assertTrue(speed <= 110.0);
        }
    }
}