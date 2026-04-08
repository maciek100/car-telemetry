package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.model.CurrentTripDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.Optional;

public interface CurrentTripRepository extends MongoRepository<CurrentTripDocument, String> {
    Optional<CurrentTripDocument> findByVin(String vin);
}
