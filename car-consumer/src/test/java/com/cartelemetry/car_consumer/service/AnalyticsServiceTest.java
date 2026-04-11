package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.model.CurrentTripDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.car_consumer.repository.CompletedTripRepository;
import com.cartelemetry.car_consumer.repository.CurrentTripRepository;
import com.cartelemetry.car_consumer.repository.SpeedAlertRepository;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.CarPositionResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.security.Timestamp;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AnalyticsServiceTest {
    @Mock
    private CarPositionRepository carPositionRepository;
    @Mock
    private CurrentTripRepository currentTripRepository;
    @Mock
    private CompletedTripRepository completedTripRepository;
    @Mock
    private SpeedAlertRepository speedAlertRepository;

    @InjectMocks
    private AnalyticsService analyticsService;

    @Test
    void haversineDistanceBetweenTwoKnownPoints() {
        // Austin to Dallas is ~300km
        double distance = analyticsService.haversine(
                30.266, -97.730,  // Austin
                32.776, -96.796   // Dallas
        );
        assertTrue(distance > 290000 && distance < 295000); // within 5km
    }

    @Test
    void haversineDistanceBetweenSamePoint() {
        double distance = analyticsService.haversine(30.266, -97.730, 30.266, -97.730);
        assertEquals(0.0D, distance); // within 5km
    }

    @Test
    void haversineShortDistance() {
        // ~1km north of Austin point
        double distance = analyticsService.haversine(
                30.266, -97.730,
                30.275, -97.730  // ~1km north
        );
        assertEquals(1000, distance, 100); // within 100 meters
    }

    @Test
    void haversineIsSymmetric() {
        double distanceAB = analyticsService.haversine(30.266, -97.730, 32.776, -96.796);
        double distanceBA = analyticsService.haversine(32.776, -96.796, 30.266, -97.730);
        assertEquals(distanceAB, distanceBA, 0.001);
    }

    @Test
    void speedPoints100mApart1Sec () {
        long  now = System.currentTimeMillis();
        double speed100m = analyticsService.computeSpeedKph(100.0, now, now + 1_000);
        assertEquals(360, speed100m);
    }

    @Test
    void speedPoints7kmApart3min () {
        long  start = System.currentTimeMillis();
        long stop = start + 3_000 * 60;

        double speed7km = analyticsService.computeSpeedKph(7000.0, start, stop);
        assertEquals(140, speed7km);
    }

    @Test
    void  speedSameLocation () {
        long  start = System.currentTimeMillis();
        long stop = start + 4_000 * 60;
        double speed0km = analyticsService.computeSpeedKph(0.0, start, stop);
        assertEquals(0, speed0km);
    }

    @Test
    void speedZeroTime () {
        long  start = System.currentTimeMillis();
        double speed0km = analyticsService.computeSpeedKph(1220.0, start, start);
        assertEquals(0, speed0km);
    }
    /*
     * New trip created for new VIN
     * Trip updates correctly with new positions
     * Speed alert created when threshold exceeded
     * Trip completes after timeout
     */

    @Test
    void newTripCreatedForNewVIN () {
        String vin = "1HGBH41JXMN109186";
        // GIVEN
        // - two unprocessed CarPositionDocuments for a VIN
        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(1000L);
        doc1.setLatitude(30.266);
        doc1.setLongitude(-97.730);
        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(11000L);
        doc2.setLatitude(30.265);
        doc2.setLongitude(-97.7285);

        List<CarPositionDocument> documents = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(documents);
        // - no existing current trip for that VIN
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.empty());


        // WHEN
        // - processVin() is called
        analyticsService.processVin(vin);

        // THEN
        // - currentTripRepository.save() was called with a new trip
        ArgumentCaptor<CurrentTripDocument> captor = ArgumentCaptor.forClass(CurrentTripDocument.class);
        verify(currentTripRepository).save(captor.capture());

        CurrentTripDocument savedTrip = captor.getValue();
        assertEquals(vin, savedTrip.getVin());
        assertEquals(30.266, savedTrip.getStartLat());
        assertEquals(-97.730, savedTrip.getStartLon());
        assertEquals(1000L, savedTrip.getTripStartTimestamp());
    }

    @Test
    void tripUpdatesCorrectlyWithNewPosition () {
        String vin = "1HGBH41JXMN109186";
        // GIVEN
        // - two unprocessed CarPositionDocuments for a VIN
        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(11000L);
        doc1.setLatitude(30.266);
        doc1.setLongitude(-97.730);
        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(21000L);
        doc2.setLatitude(30.265);
        doc2.setLongitude(-97.731);
        //CarPositionDocument doc00 = new CarPositionDocument();
        //doc0.setVin(vin);
        //doc0.setTimestamp(1000L);
        //doc0.setLatitude(30.268);
        //doc0.setLongitude(-97.728);

        List<CarPositionDocument> carPositionDocs = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(carPositionDocs);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(returnExistingTrip(vin)));


        // WHEN
        // - processVin() is called
        analyticsService.processVin(vin);

        // THEN
        // - currentTripRepository.save() was called with a new trip
        ArgumentCaptor<CurrentTripDocument> captor = ArgumentCaptor.forClass(CurrentTripDocument.class);
        verify(currentTripRepository).save(captor.capture());

        CurrentTripDocument savedTrip = captor.getValue();
        assertEquals(vin, savedTrip.getVin());
        assertEquals(30.267, savedTrip.getStartLat());
        assertEquals(-97.729, savedTrip.getStartLon());
        assertEquals( 5, savedTrip.getTotalReadings());
        assertEquals( 21000L, savedTrip.getLastUpdateTimestamp());
        assertEquals( 30.265, savedTrip.getLastLat());
        assertEquals( -97.731, savedTrip.getLastLon());
        assertEquals(703.35, savedTrip.getTotalDistanceMeters(), 1.0);
        assertEquals(44.64, savedTrip.getAverageSpeedKph());
    }

    private CurrentTripDocument returnExistingTrip(String vin) {
        CurrentTripDocument trip = new CurrentTripDocument();
        trip.setVin(vin);
        trip.setTripStartTimestamp(10);
        trip.setLastUpdateTimestamp(1000);
        trip.setStartLat(30.267);
        trip.setStartLon(-97.729);
        trip.setLastLat(30.2665);
        trip.setLastLon(-97.7299);
        trip.setTotalDistanceMeters(500);
        trip.setAverageSpeedKph(50.0);
        trip.setMaxSpeedKph(80.0);
        trip.setTotalReadings(3);
        return trip;
    }
}
