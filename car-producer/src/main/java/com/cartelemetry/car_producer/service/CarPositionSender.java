package com.cartelemetry.car_producer.service;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@RequiredArgsConstructor
public class CarPositionSender {

    private static final Logger log = LoggerFactory.getLogger(CarPositionSender.class);
    private static final String TOPIC = "car-positions";

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final CarPositionGenerator generator;

    @Scheduled(fixedRate = 1000)
    public void send() {
        generator.generateAll().forEach(carPosition -> {
            kafkaTemplate.send(TOPIC, carPosition.getVin(), carPosition.toByteArray());
            log.info("Sent CarPosition for VIN: {}", carPosition.getVin());
        });
    }
}