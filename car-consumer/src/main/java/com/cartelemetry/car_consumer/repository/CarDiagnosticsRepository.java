package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.model.CarDiagnosticsDocument;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface CarDiagnosticsRepository extends MongoRepository<CarDiagnosticsDocument, String> {
}
