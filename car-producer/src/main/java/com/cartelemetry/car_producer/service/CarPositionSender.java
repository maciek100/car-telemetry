package com.cartelemetry.car_producer.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class CarPositionSender {

    private static final Logger log = LoggerFactory.getLogger(CarPositionSender.class);
    @Value("${kafka.topic.positions}")
    private String positionsTopic;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final CarPositionGenerator generator;

    @Scheduled(fixedDelay = 1000)
    public void send() {
        long batchTimeStamp = Instant.now().toEpochMilli();;
        generator.generateAll(batchTimeStamp).forEach(carPosition ->
            kafkaTemplate.send(positionsTopic, carPosition.getVin(), carPosition.toByteArray()));
    }
}