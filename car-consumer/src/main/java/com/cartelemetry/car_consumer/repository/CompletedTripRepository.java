package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.model.CompletedTripDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface CompletedTripRepository extends MongoRepository<CompletedTripDocument, String> {
    List<CompletedTripDocument> findByVinOrderByTripStartTimestampDesc(String vin);
}