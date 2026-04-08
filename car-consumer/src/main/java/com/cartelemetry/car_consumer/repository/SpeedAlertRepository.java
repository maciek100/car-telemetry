package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.model.SpeedAlertDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface SpeedAlertRepository extends MongoRepository<SpeedAlertDocument, String> {
    List<SpeedAlertDocument> findByVinOrderByTimestampDesc(String vin);
}