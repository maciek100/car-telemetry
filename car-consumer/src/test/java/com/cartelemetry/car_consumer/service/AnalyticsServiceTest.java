package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.CarPositionDocument;
import com.cartelemetry.car_consumer.model.CompletedTripDocument;
import com.cartelemetry.car_consumer.model.CurrentTripDocument;
import com.cartelemetry.car_consumer.model.SpeedAlertDocument;
import com.cartelemetry.car_consumer.repository.CarPositionRepository;
import com.cartelemetry.car_consumer.repository.CompletedTripRepository;
import com.cartelemetry.car_consumer.repository.CurrentTripRepository;
import com.cartelemetry.car_consumer.repository.SpeedAlertRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
    void tripUpdatesCorrectlyWithNewPositionTest () {
        String vin = "1HGBH41JXMN109186";
        // GIVEN
        // - two unprocessed CarPositionDocuments for a VIN
        long timeNow = System.currentTimeMillis();
        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(timeNow + 11000L);
        doc1.setLatitude(30.266);
        doc1.setLongitude(-97.730);
        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(timeNow + 21000L);
        doc2.setLatitude(30.265);
        doc2.setLongitude(-97.731);

        List<CarPositionDocument> carPositionDocs = List.of(doc1, doc2);
        CurrentTripDocument existingTrip = buildExistingTrip(vin, timeNow);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(carPositionDocs);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(existingTrip));


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
        assertEquals( 5, savedTrip.getTotalReadings());
        assertEquals( timeNow + 21000L, savedTrip.getLastUpdateTimestamp());
        assertEquals( 30.265, savedTrip.getLastLat());
        assertEquals( -97.731, savedTrip.getLastLon());
        assertEquals(10413.93, savedTrip.getTotalDistanceMeters(), 1.0);
        assertEquals(58.35, savedTrip.getAverageSpeedKph());
    }

    @Test
    void speedAlertTest () {
        //GIVEN
        String vin = "VIN00001";
        long timeNow = System.currentTimeMillis();
        CurrentTripDocument existingTrip = buildExistingTrip(vin, timeNow);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(existingTrip));

        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(timeNow + 1000L);
        doc1.setLatitude(30.264D);
        doc1.setLongitude(-97.728D);

        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(timeNow + 2000L);
        doc2.setLatitude(30.2642D);
        doc2.setLongitude(-97.7284);
        List<CarPositionDocument> positions = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(positions);

        //WHEN
        analyticsService.processVin(vin);

        //THEN
        ArgumentCaptor<SpeedAlertDocument> captor = ArgumentCaptor.forClass(SpeedAlertDocument.class);
        verify(speedAlertRepository).save(captor.capture());
    }

    @Test
    void speedAlertTestMultipleViolations () {
        //GIVEN
        String vin = "VIN00001";
        long timeNow = System.currentTimeMillis();
        CurrentTripDocument existingTrip = buildExistingTrip(vin, timeNow);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(existingTrip));

        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(timeNow + 1000L);
        doc1.setLatitude(30.2639D);
        doc1.setLongitude(-97.728D);

        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(timeNow + 2000L);
        doc2.setLatitude(30.2640D);
        doc2.setLongitude(-97.7284);
        List<CarPositionDocument> positions = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(positions);

        //WHEN
        analyticsService.processVin(vin);

        //THEN
        ArgumentCaptor<SpeedAlertDocument> captor = ArgumentCaptor.forClass(SpeedAlertDocument.class);
        verify(speedAlertRepository, times(2)).save(captor.capture());
    }

    private CurrentTripDocument buildExistingTrip (String vin, long timeNow) {
        CurrentTripDocument existingTrip = new CurrentTripDocument();
        existingTrip.setVin(vin);
        existingTrip.setId("Fluffy");
        existingTrip.setLastLon(-97.7283D);
        existingTrip.setLastLat(30.2641D);
        existingTrip.setLastUpdateTimestamp(timeNow);
        existingTrip.setStartLat(30.266); //does not matter for this test
        existingTrip.setStartLon(-97.730); //does not matter for this test
        existingTrip.setMaxSpeedKph(50.5); //does not matter for this test
        existingTrip.setAverageSpeedKph(50.5);
        existingTrip.setTotalDistanceMeters(10000);
        existingTrip.setTotalReadings(3);
        return existingTrip;
    }

    @Test
    public void tripUpdatesWithNewPositionTest () {
        //GIVEN
        String vin = "VIN00001";
        long timeNow = System.currentTimeMillis();
        CurrentTripDocument existingTrip = buildExistingTrip(vin, timeNow);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(existingTrip));

        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(timeNow + 1000L);
        doc1.setLatitude(30.264D);
        doc1.setLongitude(-97.728D);

        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(timeNow + 2000L);
        doc2.setLatitude(30.2639D);
        doc2.setLongitude(-97.7282);
        List<CarPositionDocument> positions = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(positions);

        //WHEN
        analyticsService.processVin(vin);

        //THEN
        ArgumentCaptor<CurrentTripDocument> captor = ArgumentCaptor.forClass(CurrentTripDocument.class);
       verify(currentTripRepository).save(captor.capture());

        CurrentTripDocument savedTrip = captor.getValue();
        assertEquals(vin, savedTrip.getVin());
        assertEquals(5, savedTrip.getTotalReadings());
        assertEquals(doc2.getLatitude(), savedTrip.getLastLat());
        assertEquals(doc2.getLongitude(), savedTrip.getLastLon());
        assertEquals(doc2.getTimestamp(), savedTrip.getLastUpdateTimestamp());
        assertEquals(10054D, savedTrip.getTotalDistanceMeters(), 1.0);
    }

    @Test
    public void testHaversine () {
        double startLon = -97.799D;
        double startLat = 30.267D;
        double endLon = -97.728;
        double endLat = 30.264D;
        double distance = analyticsService.haversine(startLat, startLon, endLat, endLon);
        double speed = analyticsService.computeSpeedKph(distance, 1000, 2000);
        System.out.println(distance + " , " + speed);

    }
    @Test
    public void haversineSpeed1Test () {
        double startLon = -97.7284D;
        double endLon = -97.7286;
        double startLat = 30.26455D;
        double endLat = 30.2645D;
        double distance = analyticsService.haversine(startLat, startLon, endLat, endLon);
        double speed = analyticsService.computeSpeedKph(distance, 1000, 2000);
        assertEquals(19.99D, distance, 1.0D);
        assertEquals(71.99D, speed, 1.0D);
    }

    @Test
    public void haversineSpeed2Test () {
        double startLon = -97.728;
        double endLon =   -97.7281;
        double startLat = 30.264D;
        double endLat = 30.2642D;
        double distance = analyticsService.haversine(startLat, startLon, endLat, endLon);
        double speed = analyticsService.computeSpeedKph(distance, 1000, 2000);
        System.out.println(distance + "   " + speed);
        assertEquals(24.0D, distance, 1.0D);
        assertEquals(87.0D, speed, 1.0D);

    }

    @Test
    public void haversineHighSpeedTest () {
        //less than 300kph (GPS anomaly)
        //more than SPEED_LIMIT_KPH ... currently 120.
        double startLon = -97.7280;
        double endLon   = -97.7285;
        double startLat = 30.264D;
        double endLat   = 30.2642D;
        double distance = analyticsService.haversine(startLat, startLon, endLat, endLon);
        double speed = analyticsService.computeSpeedKph(distance, 1000, 2000);
        assertEquals(53.0D, distance, 1.0D);
        assertEquals(190.0D, speed, 1.0D);
    }

    @Test
    public void tripCompletesAfterTimeout () {
        //GIVEN
        long timeNow = System.currentTimeMillis();
        String vin = "VIN0011";
        CurrentTripDocument currentTrip = buildExistingTrip(vin, timeNow - 700_000);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(currentTrip));
        //when(carPositionRepository.findByVin(vin)).thenReturn(List.of());

        //WHEN
        analyticsService.processVin(vin);

        //THEN
        ArgumentCaptor<CompletedTripDocument> captor = ArgumentCaptor.forClass(CompletedTripDocument.class);
        verify(completedTripRepository).save(captor.capture());
        assertEquals(vin, captor.getValue().getVin());
        ArgumentCaptor<CurrentTripDocument> captor2 = ArgumentCaptor.forClass(CurrentTripDocument.class);
        verify(currentTripRepository).delete(captor2.capture());
    }

    @Test
    public void outOfOrderDocumentsAreDiscardedTest () {
        //GIVEN
        long timeNow = System.currentTimeMillis();
        String vin = "VIN0012";
        CurrentTripDocument existingTrip = buildExistingTrip(vin, timeNow + 5_000);
        when(currentTripRepository.findByVin(vin)).thenReturn(Optional.of(existingTrip));

        CarPositionDocument doc1 = new CarPositionDocument();
        doc1.setVin(vin);
        doc1.setTimestamp(timeNow + 1000L);
        doc1.setLatitude(30.264D);
        doc1.setLongitude(-97.728D);

        CarPositionDocument doc2 = new CarPositionDocument();
        doc2.setVin(vin);
        doc2.setTimestamp(timeNow + 2000L);
        doc2.setLatitude(30.2639D);
        doc2.setLongitude(-97.7282);
        List<CarPositionDocument> positions = List.of(doc1, doc2);
        when(carPositionRepository.findByVinAndProcessedFalseOrderByTimestampAsc(vin)).thenReturn(positions);

        //WHEN
        analyticsService.processVin(vin);

        //THEN
        verify(currentTripRepository, never()).save(any());
    }

    @Test
    public void testTimeBeautifiers () {
        long timeNow = System.currentTimeMillis();
        long timeThen = timeNow - (4 * 3600L * 1000_000_000 + 13 * 60L * 1000_000 + 19 * 1000 +  456);
        System.out.println(AnalyticsService.prettyDuration.apply(timeNow - timeThen));
        System.out.println(AnalyticsService.prettyTime.apply(timeNow));
    }
}
