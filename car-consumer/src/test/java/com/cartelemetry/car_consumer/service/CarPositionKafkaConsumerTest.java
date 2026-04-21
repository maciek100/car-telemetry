package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DuplicateKeyException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CarPositionKafkaConsumerTest {

    @Mock
    private CarPositionRepository repository;

    @InjectMocks
    private CarPositionKafkaConsumer consumer;

    @Test
    void validMessageSavesToMongoDB() {
        // GIVEN
        String vin = "V123ABC456DEF";
        long timeNow = System.currentTimeMillis();
        CarPosition carPosition = CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(timeNow)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(30.266)
                        .setLongitude(-97.730)
                        .build())
                .setSpeed(50.5)
                .setHeading(127.5)
                .build();
        byte[] payLoad = carPosition.toByteArray();
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "car-position",
                0, 0L,
                vin, payLoad);

        // WHEN
        consumer.consume(record);

        // THEN
        ArgumentCaptor<CarPositionDocument> captor = ArgumentCaptor.forClass(CarPositionDocument.class);
        verify(repository).save(captor.capture());
        //verify(repository).save(any(CarPositionDocument.class));
        CarPositionDocument saved = captor.getValue();
        assertEquals(vin, saved.getVin());
        assertEquals(timeNow, saved.getTimestamp());
        assertEquals(30.266, saved.getLatitude());
        assertEquals(-97.730, saved.getLongitude());
        assertEquals(50.5, saved.getSpeed());
        assertEquals(127.5, saved.getHeading());
        assertFalse(saved.isProcessed());
    }

    @Test
    public void testDuplicateMessage () {
        String vin = "V123ABC456DEF";
        long timeNow = System.currentTimeMillis();
        CarPosition carPosition = CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(timeNow)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(30.266)
                        .setLongitude(-97.730)
                        .build())
                .setSpeed(50.5)
                .build();
        byte[] payLoad = carPosition.toByteArray();
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "car-position",
                0, 0L,
                vin, payLoad);
        when(repository.save(any(CarPositionDocument.class)))
                .thenReturn(null)
                .thenThrow(new DuplicateKeyException("duplicate"));
        // WHEN
        consumer.consume(record);
        consumer.consume(record);

        //THEN
        verify(repository, times(2)).save(any(CarPositionDocument.class));
    }

    @Test
    public void testInvalidBytesHandling () {
        ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(
                "car-positions",
                0, 0L,
                "VIN000000",
                "not valid protobuf".getBytes());  // garbage bytes

        // WHEN
        consumer.consume(record);  // should NOT throw!

        // THEN
        verify(repository, never()).save(any());  // save never called
    }
}