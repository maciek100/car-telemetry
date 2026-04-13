package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.CarPositionResponse;
import com.cartelemetry.proto.CarPositionServiceGrpc;
import org.springframework.dao.DuplicateKeyException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrpcService
@RequiredArgsConstructor
public class CarPositionGrpcService extends CarPositionServiceGrpc.CarPositionServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(CarPositionGrpcService.class);

    private final CarPositionRepository repository;

    @Override
    public void sendCarPosition(CarPosition request, StreamObserver<CarPositionResponse> responseObserver) {
        log.info("Received CarPosition for VIN: {}", request.getVin());
        try {
            CarPositionDocument doc = new CarPositionDocument();
            doc.setVin(request.getVin());
            doc.setTimestamp(request.getTimestamp());
            doc.setLatitude(request.getLocation().getLatitude());
            doc.setLongitude(request.getLocation().getLongitude());
            doc.setSpeed(request.getSpeed());
            doc.setEngineTemp(request.getEngineTemp());
            doc.setGasTankLevel(request.getGasTankLevel());
            doc.setObd2ErrorCode(request.getObd2ErrorCode());


            repository.save(doc);
            responseObserver.onNext(CarPositionResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Saved successfully")
                    .build());

        } catch (DuplicateKeyException e) {
                log.warn("Duplicate position ignored for VIN: {} timestamp: {}",
                        request.getVin(), request.getTimestamp());

            responseObserver.onNext(CarPositionResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Duplicate position ignored")
                    .build());
        } catch (Exception e) {
            log.error("Error saving CarPosition", e);
            responseObserver.onError(e);
        } finally {
            responseObserver.onCompleted();
        }
    }
}