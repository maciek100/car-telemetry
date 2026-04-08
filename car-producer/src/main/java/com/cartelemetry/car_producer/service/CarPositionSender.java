package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.CarPositionServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class CarPositionSender {
    private static final Logger log = LoggerFactory.getLogger(CarPositionSender.class);

    @GrpcClient("car-consumer")
    private CarPositionServiceGrpc.CarPositionServiceBlockingStub stub;

    private final CarPositionGenerator generator;

    public CarPositionSender(CarPositionGenerator generator) {
        this.generator = generator;
    }

    @Scheduled(fixedRate = 5000)
    public void send() {
        CarPosition carPosition = generator.generate();
        if (carPosition == null) {
            log.info("Noting to move");
            return;
        }
        var response = stub.sendCarPosition(carPosition);
        log.info("Sent carPosition for VIN: {} response: {}", carPosition.getVin(), response.getMessage());
    }
}