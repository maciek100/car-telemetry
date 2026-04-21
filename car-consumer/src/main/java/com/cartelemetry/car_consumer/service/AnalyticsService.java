package com.cartelemetry.car_consumer.service;

import com.cartelemetry.car_consumer.model.*;
import com.cartelemetry.car_consumer.repository.*;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);
    private final MongoTemplate mongoTemplate;
    private static final double SPEED_LIMIT_KPH = 120.0;
    private static final long TRIP_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes

    private final CarPositionRepository carPositionRepository;
    private final CurrentTripRepository currentTripRepository;
    private final CompletedTripRepository completedTripRepository;
    private final SpeedAlertRepository speedAlertRepository;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile long lastTriggeredAt = 0;

    public static Function<Long, String> prettyTime = number -> Instant.ofEpochMilli(number)
            .atZone(ZoneId.systemDefault())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

    public static Function<Long, String> prettyDuration = millis -> {
        long hours = millis / 3_600_000;
        long minutes = (millis % 3_600_000) / 60_000;
        long seconds = (millis % 60_000) / 1_000;
        long ms = millis % 1_000;

        if (hours > 0)
            return String.format("%dh%02dm%02ds", hours, minutes, seconds);
        if (minutes > 0)
            return String.format("%dm%02ds%03d", minutes, seconds, ms);
        if (seconds > 0)
            return String.format("%ds%03dms", seconds, ms);
        return String.format("%dms", ms);
    };

    public static String formatuuuDuration(long millis) {
        long hours = millis / 3_600_000;
        long minutes = (millis % 3_600_000) / 60_000;
        long seconds = (millis % 60_000) / 1_000;
        long ms = millis % 1_000;

        if (hours > 0)
            return String.format("%dh%02dm%02ds", hours, minutes, seconds);
        if (minutes > 0)
            return String.format("%dm%02ds%03d", minutes, seconds, ms);
        if (seconds > 0)
            return String.format("%ds%03dms", seconds, ms);
        return String.format("%dms", ms);
    }
    /**
     * Scheduled runs are synchronous - called directly from scheduledAnalytics()
     * Manual triggers via REST API are async - annotated with @Async
     * This separation ensures scheduled runs don't block and manual triggers
     * return immediately to the caller.
     * See AnalyticsController.triggerProcessing() for manual trigger implementation.
     */
    @Scheduled(fixedRateString = "${analytics.schedule.rate:600000}") // every 5 minutes
    public void scheduledAnalytics () {
        processAnalytics();
    }
    @Async
    public void processAnalytics() {
        if (!running.compareAndSet(false, true)) {
            log.info("Analytics already running, skipping...");
            return;
        }
        lastTriggeredAt = System.currentTimeMillis();
        try {
            log.info("Analytics started at {}", prettyTime.apply(System.currentTimeMillis()));
            mongoTemplate.findDistinct("vin", CarPositionDocument.class, String.class)
                    .forEach(this::processVin);
            currentTripRepository.findAll()
                            .forEach(trip -> checkTripTimeout(trip.getVin()));
            log.info("Analytics completed in {}ms", prettyDuration.apply(System.currentTimeMillis() - lastTriggeredAt));
        } finally {
            running.set(false);
        }
    }
    void processVin (String vin) {
        List<CarPositionDocument> unprocessed = getUnprocessedDocs(vin);
        if (unprocessed.isEmpty()) {
            checkTripTimeout(vin);
            return;
        }

        Optional<CurrentTripDocument> existingTrip = currentTripRepository.findByVin(vin);

        prepareDocumentList(vin, existingTrip, unprocessed);
        if (unprocessed.size() <= 1) {
            checkTripTimeout(vin);
            return;
        }

        CurrentTripDocument currentTrip = existingTrip
                .orElseGet(() -> createNewTrip(vin, unprocessed.getFirst()));

        processSegments(vin, unprocessed, currentTrip);
        markLastDocAsProcessed(unprocessed);
        currentTripRepository.save(currentTrip);

        log.info("VIN: {} trip distance: {}m avg speed: {}kph max speed: {}kph",
                vin,
                String.format("%.2f", currentTrip.getTotalDistanceMeters()),
                String.format("%.2f", currentTrip.getAverageSpeedKph()),
                String.format("%.2f", currentTrip.getMaxSpeedKph()));
    }

    private List<CarPositionDocument> getUnprocessedDocs(String vin) {
        return new ArrayList<>(carPositionRepository
                .findByVinAndProcessedFalseOrderByTimestampAsc(vin));
    }

    private void prepareDocumentList (String vin,
                                      Optional<CurrentTripDocument> currentTrip,
                                      List<CarPositionDocument> unprocessed) {
        //discard any LATE arriving documents ...
        // these are the docs which were recorded BEFORE the currentExistingTrip concluded
        currentTrip.ifPresent( trip ->
                unprocessed.removeIf(doc ->
                        doc.getTimestamp() <= trip.getLastUpdateTimestamp()));

        //build the bridge to pre-existing unexpired trip
        currentTrip.ifPresent(trip -> {
            CarPositionDocument bridge = new CarPositionDocument();
            bridge.setVin(vin);
            bridge.setLatitude(trip.getLastLat());
            bridge.setLongitude(trip.getLastLon());
            bridge.setTimestamp(trip.getLastUpdateTimestamp());
            bridge.setProcessed(true);
            unprocessed.add(0,bridge);
        });


    }

    private void processSegments (String vin, List<CarPositionDocument> unprocessed, CurrentTripDocument currentTrip) {
        for (int i = 0; i < unprocessed.size() - 1; i++) {
            CarPositionDocument from = unprocessed.get(i);
            CarPositionDocument to = unprocessed.get(i + 1);

            double distanceMeters = haversine(
                    from.getLatitude(), from.getLongitude(),
                    to.getLatitude(), to.getLongitude()
            );

            double computedSpeedKph = computeSpeedKph(distanceMeters, from.getTimestamp(), to.getTimestamp());
            double timeDeltaSeconds = (to.getTimestamp() - from.getTimestamp()) / 1000.0;
            if (computedSpeedKph > 300) {  // suspicious threshold
                log.warn("SUSPICIOUS SPEED [GPS anomaly] for VIN: {} speed: {}kph distance: {}m timeDelta: {}s from: ({},{}) to: ({},{})",
                        vin,
                        String.format("%.2f", computedSpeedKph),
                        String.format("%.2f", distanceMeters),
                        timeDeltaSeconds,
                        from.getLatitude(), from.getLongitude(),
                        to.getLatitude(), to.getLongitude()
                );
                continue;
            }
            if (distanceMeters > 1000 && timeDeltaSeconds < 10) {
                log.warn("SUSPICIOUS POSITION JUMP [GPS anomaly] for VIN: {} distance: {}m in {}s",
                        vin, String.format("%.2f", distanceMeters), timeDeltaSeconds);
                continue;
            }
            updateTrip(currentTrip, to, distanceMeters, computedSpeedKph);

            if (!from.isProcessed()) {
                from.setProcessed(true);
                carPositionRepository.save(from);
            }
        }
    }

    private void markLastDocAsProcessed (List<CarPositionDocument> unprocessed) {
        // mark last document as processed
        CarPositionDocument last = unprocessed.get(unprocessed.size() - 1);
        if(!last.isProcessed()) {
            last.setProcessed(true);
            carPositionRepository.save(last);
        }
    }

    /**
     * given a VIN system checks if there is an existing _active_ trip for this vehicle.
     * If so it pre-appends the list of unprocessed CarPositionDocument(s) with a "bridge" CarPositionDocument
     * to link the new positions with the existing trip.
     * If not it would pre-append an empty CarPositionDocument.
     */
    private CurrentTripDocument buildBridge(String vin, List<CarPositionDocument> unprocessed) {
        Optional<CurrentTripDocument> existingTrip = currentTripRepository.findByVin(vin);
        existingTrip.ifPresent(trip -> {
            CarPositionDocument bridge = new CarPositionDocument();
            bridge.setVin(vin);
            bridge.setLatitude(trip.getLastLat());
            bridge.setLongitude(trip.getLastLon());
            bridge.setTimestamp(trip.getLastUpdateTimestamp());
            bridge.setProcessed(true);
            unprocessed.add(0,bridge);
        });
        CurrentTripDocument currentTrip = existingTrip
                .orElseGet(() -> createNewTrip(vin, unprocessed.getFirst()));

        return currentTrip;
    }

    private void updateTrip (CurrentTripDocument currentTrip, CarPositionDocument to, double distanceMeters, double computedSpeedKph) {
        currentTrip.setTotalDistanceMeters(
                currentTrip.getTotalDistanceMeters() + distanceMeters);
        currentTrip.setTotalReadings(currentTrip.getTotalReadings() + 1);
        currentTrip.setLastUpdateTimestamp(to.getTimestamp());
        currentTrip.setLastLat(to.getLatitude());
        currentTrip.setLastLon(to.getLongitude());

        // update max speed
        if (computedSpeedKph > currentTrip.getMaxSpeedKph())
            currentTrip.setMaxSpeedKph(Math.round(computedSpeedKph * 100.0)/ 100.0);

        // update average speed
        currentTrip.setAverageSpeedKph(Math.round(
                (currentTrip.getAverageSpeedKph() * (currentTrip.getTotalReadings() - 1)
                        + computedSpeedKph) / currentTrip.getTotalReadings()
                        * 100.0)/ 100.0);

        // check speed alert
        if (computedSpeedKph > SPEED_LIMIT_KPH) {
            createSpeedAlert(currentTrip.getVin(), to, Math.round(computedSpeedKph * 100.0)/ 100.0);
        }
    }

    private void checkTripTimeout(String vin) {
        currentTripRepository.findByVin(vin).ifPresent(trip -> {
            long now = System.currentTimeMillis();
            long diff = now - trip.getLastUpdateTimestamp();
            log.info("JACK1 VIN: {} last update {}ms ago, timeout at {}ms",
                    vin, diff, TRIP_TIMEOUT_MS);
            if (diff > TRIP_TIMEOUT_MS) {
                log.info("JACK2 Trip timeout for VIN: {} — completing trip", vin);
                completeTrip(trip);
            }
        });
    }

    private void completeTrip(CurrentTripDocument current) {
        CompletedTripDocument completed = new CompletedTripDocument();
        completed.setVin(current.getVin());
        completed.setTripStartTimestamp(current.getTripStartTimestamp());
        completed.setTripEndTimestamp(current.getLastUpdateTimestamp());
        completed.setStartLat(current.getStartLat());
        completed.setStartLon(current.getStartLon());
        completed.setEndLat(current.getLastLat());
        completed.setEndLon(current.getLastLon());
        completed.setTotalDistanceMeters(current.getTotalDistanceMeters());
        completed.setAverageSpeedKph(current.getAverageSpeedKph());
        completed.setMaxSpeedKph(current.getMaxSpeedKph());
        completed.setTotalReadings(current.getTotalReadings());
        completed.setDurationMinutes(
                (current.getLastUpdateTimestamp() - current.getTripStartTimestamp()) / 60000);

        completedTripRepository.save(completed);
        currentTripRepository.delete(current);
        log.info("Trip completed for VIN: {}", current.getVin());
    }

    private CurrentTripDocument createNewTrip(String vin, CarPositionDocument first) {
        log.info("Starting new trip for VIN: {}", vin);
        CurrentTripDocument trip = new CurrentTripDocument();
        trip.setVin(vin);
        trip.setTripStartTimestamp(first.getTimestamp());
        trip.setLastUpdateTimestamp(first.getTimestamp());
        trip.setStartLat(first.getLatitude());
        trip.setStartLon(first.getLongitude());
        trip.setLastLat(first.getLatitude());
        trip.setLastLon(first.getLongitude());
        trip.setTotalDistanceMeters(0);
        trip.setAverageSpeedKph(0);
        trip.setMaxSpeedKph(0);
        trip.setTotalReadings(0);
        return trip;
    }

    private void createSpeedAlert(String vin, CarPositionDocument doc, double computedSpeedKph) {
        log.warn("Speed alert for VIN: {} computed speed: {}kph", vin,
                String.format("%.2f", computedSpeedKph));
        SpeedAlertDocument alert = new SpeedAlertDocument();
        alert.setVin(vin);
        alert.setTimestamp(doc.getTimestamp());
        alert.setLatitude(doc.getLatitude());
        alert.setLongitude(doc.getLongitude());
        alert.setComputedSpeedKph(computedSpeedKph);
        alert.setSpeedLimitKph(SPEED_LIMIT_KPH);
        speedAlertRepository.save(alert);
    }

    double haversine(double lat1, double lon1, double lat2, double lon2) {
        double R = 6371000; // Earth radius in meters
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon/2) * Math.sin(dLon/2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }

    double computeSpeedKph(double distanceMeters, long fromTimestamp, long toTimestamp) {
        double timeDeltaSeconds = (toTimestamp - fromTimestamp) / 1000.0;
        if (timeDeltaSeconds <= 0) return 0;
        return (distanceMeters / timeDeltaSeconds) * 3.6;
    }

    public boolean isRunning () {
        return running.get();
    }

    public long getLastTriggeredAt () {
        return lastTriggeredAt;
    }
}