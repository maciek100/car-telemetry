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
import org.springframework.beans.factory.annotation.Value;

@Service
@RequiredArgsConstructor
public class CarPositionKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(CarPositionKafkaConsumer.class);
    private final CarPositionRepository repository;

    @KafkaListener(topics = "${kafka.topic.positions}", groupId = "${kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, byte[]> record) {
        try {
            CarPosition carPosition = CarPosition.parseFrom(record.value());
            CarPositionDocument cpDoc = new CarPositionDocument();
            cpDoc.setVin(carPosition.getVin());
            cpDoc.setTimestamp(carPosition.getTimestamp());
            cpDoc.setLatitude(carPosition.getLocation().getLatitude());
            cpDoc.setLongitude(carPosition.getLocation().getLongitude());
            cpDoc.setSpeed(carPosition.getSpeed());
            cpDoc.setHeading(carPosition.getHeading());

            try {
                repository.save(cpDoc);
                log.info("Saved CarPosition for VIN: {}", cpDoc.getVin());
            } catch (DuplicateKeyException e) {
                log.warn("Duplicate position ignored for VIN: {} timestamp: {}",
                        cpDoc.getVin(), cpDoc.getTimestamp());
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize CarPosition from topic: {} offset: {}",
                    record.topic(), record.offset(), e);
        }
    }
}