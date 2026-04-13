package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarPositionServiceGrpc;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class CarPositionSender {
    private static final Logger log = LoggerFactory.getLogger(CarPositionSender.class);

    @GrpcClient("car-consumer")
    private CarPositionServiceGrpc.CarPositionServiceBlockingStub stub;

    private final CarPositionGenerator generator;

    public CarPositionSender(CarPositionGenerator generator) {
        this.generator = generator;
    }

    @Scheduled(fixedRate = 1000)
    public void send() {

        generator.generateAll()
                .forEach(carPosition -> {
                    var response = stub.sendCarPosition(carPosition);
                    if (response.getSuccess())
                        log.info("Sent carPosition for VIN: {} response: {}", carPosition.getVin(), response.getMessage());
                    else {
                        log.warn("Position not sent for VIN: {} reason: {}",
                                carPosition.getVin(), response.getMessage());
                    }
                });
    }
}