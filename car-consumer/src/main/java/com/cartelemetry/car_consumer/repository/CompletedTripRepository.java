package com.cartelemetry.car_consumer.repository;

import com.cartelemetry.car_consumer.dto.TripAggregationResult;
import com.cartelemetry.car_consumer.model.CompletedTripDocument;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface CompletedTripRepository extends MongoRepository<CompletedTripDocument, String> {
    List<CompletedTripDocument> findByVinOrderByTripStartTimestampDesc(String vin);
    //sum total distance and avgSpeed for VIN
    @Aggregation(pipeline = {
            "{ $match: { vin: ?0 } }",
            "{ $group: { _id: null, " +
                    "totalDistance: { $sum: '$totalDistanceMeters' }, " +
                    "avgSpeed: { $avg: '$averageSpeedKph' } } }"
    })
    TripAggregationResult getTripStatsByVin(String vin);
    long countByVin(String vin);
}