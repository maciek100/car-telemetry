package com.cartelemetry.car_producer.service;

import com.cartelemetry.proto.CarDiagnostics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class CarDiagnosticSender {
    private static final Logger log = LoggerFactory.getLogger(CarDiagnosticSender.class);
    @Value("${kafka.topic.diagnostics}")
    private static String diagnosticsTopic;

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final CarDiagnosticsGenerator generator;

    public CarDiagnosticSender(KafkaTemplate<String, byte[]> kafkaTemplate, CarDiagnosticsGenerator generator) {
        this.kafkaTemplate = kafkaTemplate;
        this.generator = generator;
    }

    @Scheduled(fixedDelay = 20000)
    public void send() {
        CarDiagnostics carDiag = generator.generateDiagnostics();

            kafkaTemplate.send(diagnosticsTopic, carDiag.getVin(), carDiag.toByteArray());
            log.info("Sent CarDiagnostics for VIN: {}", carDiag.getVin());
    }
}
