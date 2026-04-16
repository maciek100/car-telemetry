package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.proto.CarPosition;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CarPositionKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(CarPositionKafkaConsumer.class);
    private final CarPositionRepository repository;

    @KafkaListener(topics = "car-positions", groupId = "car-telemetry-group")
    public void consume(ConsumerRecord<String, byte[]> record) {
        try {
            CarPosition carPosition = CarPosition.parseFrom(record.value());
            CarPositionDocument doc = new CarPositionDocument();
            doc.setVin(carPosition.getVin());
            doc.setTimestamp(carPosition.getTimestamp());
            doc.setLatitude(carPosition.getLocation().getLatitude());
            doc.setLongitude(carPosition.getLocation().getLongitude());
            doc.setSpeed(carPosition.getSpeed());
            doc.setEngineTemp(carPosition.getEngineTemp());
            doc.setGasTankLevel(carPosition.getGasTankLevel());
            doc.setObd2ErrorCode(carPosition.getObd2ErrorCode());

            try {
                repository.save(doc);
                log.info("Saved CarPosition for VIN: {}", doc.getVin());
            } catch (DuplicateKeyException e) {
                log.warn("Duplicate position ignored for VIN: {} timestamp: {}",
                        doc.getVin(), doc.getTimestamp());
            }

            log.info("Saved CarPosition for VIN: {}", doc.getVin());
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize CarPosition from topic: {} offset: {}",
                    record.topic(), record.offset(), e);
        }
    }
}