package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;

public interface CarPositionRepository extends MongoRepository<CarPositionDocument, String> {
    List<CarPositionDocument> findByVinAndProcessedFalseOrderByTimestampAsc(String vin);
    List<CarPositionDocument> findByVin(String vin);
    //@Query(value = "{}", fields = "{'vin':1}")
    //List<CarPositionDocument> findDistinctVins();
    List<String> findDistinctVinBy();
}