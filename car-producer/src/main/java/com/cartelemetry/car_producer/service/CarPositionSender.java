package com.cartelemetry.car_producer.service;

import org.springframework.beans.factory.annotation.Value;
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
    @Value("${kafka.topic.positions}")
    private static String positionsTopic;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final CarPositionGenerator generator;

    @Scheduled(fixedDelay = 1000)
    public void send() {
        generator.generateAll().forEach(carPosition -> {
            kafkaTemplate.send(positionsTopic, carPosition.getVin(), carPosition.toByteArray());
            log.info("Sent CarPosition for VIN: {}", carPosition.getVin());
        });
    }
}