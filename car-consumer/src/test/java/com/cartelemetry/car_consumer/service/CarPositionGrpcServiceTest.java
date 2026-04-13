package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.CarPositionResponse;
import com.cartelemetry.proto.GpsLocation;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import org.springframework.dao.DuplicateKeyException;

@ExtendWith(MockitoExtension.class)
public class CarPositionGrpcServiceTest {
    @Mock
    private CarPositionRepository carPositionRepository;
    @InjectMocks
    private CarPositionGrpcService carPositionGrpcService;

    @Test
    void duplicatePositionReturnsFailureResponse() {
        // GIVEN
        CarPosition request = CarPosition.newBuilder()
                .setVin("VIN000000")
                .setTimestamp(1000L)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(30.266)
                        .setLongitude(-97.730)
                        .build())
                .build();

        // simulate duplicate key exception from MongoDB
        when(carPositionRepository.save(any(CarPositionDocument.class)))
                .thenThrow(new DuplicateKeyException("duplicate key error"));

        StreamObserver<CarPositionResponse> responseObserver = mock(StreamObserver.class);

        // WHEN
        carPositionGrpcService.sendCarPosition(request, responseObserver);

        // THEN
        ArgumentCaptor<CarPositionResponse> captor =
                ArgumentCaptor.forClass(CarPositionResponse.class);
        verify(responseObserver).onNext(captor.capture());

        assertFalse(captor.getValue().getSuccess());
        assertEquals("Duplicate position ignored", captor.getValue().getMessage());
        verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }
}
