package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarDiagnosticsDocument;
import com.cartelemetry.car_consumer.repository.CarDiagnosticsRepository;
import com.cartelemetry.proto.CarDiagnostics;
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
public class CarDiagnosticsKafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(CarPositionKafkaConsumer.class);
    private final CarDiagnosticsRepository repository;

    @KafkaListener(topics = "${kafka.topic.positions}", groupId = "${kafka.consumer.group-id}")
    public void consume(ConsumerRecord<String, byte[]> record) {
        try {
            CarDiagnostics carDiag = CarDiagnostics.parseFrom(record.value());
            CarDiagnosticsDocument carDiagDoc = new CarDiagnosticsDocument();
            carDiagDoc.setVin(carDiag.getVin());
            carDiagDoc.setTimestamp(carDiag.getTimestamp());
            carDiagDoc.setEngineTemp(carDiag.getEngineTemp());
            carDiagDoc.setGasTankLevel(carDiag.getFuelLevel());
            carDiagDoc.setObd2ErrorCode(carDiag.getObd2ErrorCodesList());
            carDiagDoc.setBatteryVoltage(carDiag.getBatteryVoltage());
            carDiagDoc.setOilPressure(carDiag.getOilPressure());
            carDiagDoc.setRpm(carDiag.getRpm());
            carDiagDoc.setOdometer(carDiag.getOdometer());
            carDiagDoc.setTirePressureRR(carDiag.getTirePressureRR());
            carDiagDoc.setTirePressureFR(carDiag.getTirePressureFR());
            carDiagDoc.setTirePressureRL(carDiag.getTirePressureRL());
            carDiagDoc.setTirePressureFL(carDiag.getTirePressureFL());

            try {
                repository.save(carDiagDoc);
                log.info("Saved CarDiagnostics for VIN: {}", carDiagDoc.getVin());
            } catch (DuplicateKeyException e) {
                log.warn("Duplicate diagnostics ignored for VIN: {} timestamp: {}",
                        carDiagDoc.getVin(), carDiagDoc.getTimestamp());
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to deserialize CarDiagnostics from topic: {} offset: {}",
                    record.topic(), record.offset(), e);
        }
    }

}
