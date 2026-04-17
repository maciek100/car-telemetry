package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CarPositionSenderTest {

    @Mock
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Mock
    private CarPositionGenerator generator;

    @InjectMocks
    private CarPositionSender sender;

    @Test
    void sendsOneMessagePerVehicle() {
        // GIVEN
        // mock generator to return 3 CarPositions
        CarPosition cp1 = moveCar(null);
        CarPosition cp2 = moveCar(cp1);
        CarPosition cp3 = moveCar(cp2);
        CarPosition cp4 = moveCar(cp3);
        var movement = List.of(cp1, cp2, cp3, cp4);
        when(generator.generateAll()).thenReturn(movement);
        // WHEN
        sender.send();
        // THEN
        verify(kafkaTemplate, times(4)).send(eq("car-positions"), anyString(), any(byte[].class));
    }

    private CarPosition moveCar(CarPosition cp) {
        if (cp == null)
            return CarPosition.newBuilder()
                .setVin("VIN123")
                .setTimestamp(10000000)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(30.266)
                        .setLongitude(-97.730)
                        .build())
                .setSpeed(50.5)
                .setEngineTemp(195.0)
                .setGasTankLevel(0.75)
                .setObd2ErrorCode("")
                .build();
        return CarPosition.newBuilder()
                .setVin(cp.getVin())
                .setTimestamp(cp.getTimestamp() + 1005)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(cp.getLocation().getLatitude() + 0.0015)
                        .setLongitude(cp.getLocation().getLongitude() + 0.001)
                        .build())
                .setSpeed(cp.getSpeed())
                .setEngineTemp(cp.getEngineTemp())
                .setGasTankLevel(cp.getGasTankLevel())
                .setObd2ErrorCode(cp.getObd2ErrorCode())
                .build();
    }
}