package com.cartelemetry.car_flink.functions;

import com.cartelemetry.car_flink.util.GPSUtil;
import com.cartelemetry.car_flink.util.MongoUtil;
import com.cartelemetry.proto.CarPosition;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class CarPositionProcessFunction
        extends KeyedProcessFunction<String, byte[], String> {

    // state per VIN
    private ValueState<Double> lastLatState;
    private ValueState<Double> lastLonState;
    private ValueState<Long> lastTimestampState;
    private ValueState<Double> totalDistanceState;
    private ValueState<Integer> totalReadingsState;
    private ValueState<Long> tripStartTimestampState;
    private ValueState<Double> startLatState;
    private ValueState<Double> startLonState;
    private ValueState<Double> maxSpeedState;
    private ValueState<Long> timerState;  // tracks current timer

    private MapState<Long, Boolean> seenTimeStamps;

    //private transient MongoClient mongoClient;
    private transient MongoCollection<Document> completedTripsCollection;
    private transient MongoCollection<Document> speedAlertsCollection;

    private static final long TRIP_TIMEOUT_MS = 60 * 1000;  // 5 minutes
    private static final double SPEED_LIMIT_KPH = 120.0;
    private static final double SPEED_ANOMALY_KPH = 300.0;
    private static final long NEW_TRIP_THRESHOLD_MS = 3000;
    private long flinkStartTime;

    private final String completedTripsCollectionName;
    private final String speedAlertsCollectionName;

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(CarPositionProcessFunction.class);

    public CarPositionProcessFunction() {
        this("flink_completed_trips","flink_speed_alerts");
    }

    public CarPositionProcessFunction(String completedTripsCollectionName, String speedAlertsCollectionName) {
        this.completedTripsCollectionName = completedTripsCollectionName;
        this.speedAlertsCollectionName = speedAlertsCollectionName;
    }

   public CarPositionProcessFunction(MongoCollection<Document> completedTripsCollection, MongoCollection<Document> speedAlertsCollection) {
       this.completedTripsCollectionName = null;
       this.speedAlertsCollectionName = null;
        this.completedTripsCollection = completedTripsCollection;
        this.speedAlertsCollection = speedAlertsCollection;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        if (completedTripsCollection == null) {
            completedTripsCollection = MongoUtil.getCollection(completedTripsCollectionName);
            speedAlertsCollection = MongoUtil.getCollection(speedAlertsCollectionName);
        }
        flinkStartTime = System.currentTimeMillis() + 15000;

        seenTimeStamps = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("seenTimeStamps", Long.class, Boolean.class));
        lastLatState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastLat", Double.class));
        lastLonState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastLon", Double.class));
        lastTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("lastTimestamp", Long.class));
        totalDistanceState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalDistance", Double.class));
        totalReadingsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalReadings", Integer.class));
        tripStartTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("tripStartTimestamp", Long.class));
        startLatState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("startLat", Double.class));
        startLonState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("startLon", Double.class));
        maxSpeedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("maxSpeed", Double.class));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("timer", Long.class));
    }

    @Override
    public void processElement(byte[] value, Context ctx,
                               Collector<String> out) throws Exception {

        CarPosition position = CarPosition.parseFrom(value);
        long timestamp = position.getTimestamp();
        String vin = position.getVin();
        if (isDuplicate(timestamp)) return;
        if (timestamp < flinkStartTime) return;

        resetTimer(ctx);

        Long lastTimestamp = lastTimestampState.value();
        if (isNewTrip(lastTimestamp, timestamp)) {
            // NEW TRIP!
            startNewTrip(vin, position, timestamp, out);
        } else {
            // CONTINUING TRIP
            continueTrip(vin, position, timestamp, lastTimestamp, out);
        }
        updatePosition(position, timestamp);
        registerTimer(ctx);
    }

    /**
     * Checks if this position message is a duplicate.
     * Cleans up timestamps older than 10 seconds
     * to prevent MapState from growing indefinitely.
     */
    private boolean isDuplicate(long timestamp) throws Exception {
        // Clean old timestamps
        long cutoff = timestamp - 10000;
        List<Long> toRemove = new ArrayList<>();
        for (Long ts : seenTimeStamps.keys()) {
            if (ts < cutoff) toRemove.add(ts);
        }
        for (Long ts : toRemove) {
            seenTimeStamps.remove(ts);
        }

        // Check duplicate
        if (seenTimeStamps.contains(timestamp)) return true;
        seenTimeStamps.put(timestamp, true);
        return false;
    }
    /**
     * Determines if gap between messages indicates
     * a new trip has started.
     * Gap > NEW_TRIP_THRESHOLD_MS = new trip.
     */
    private boolean isNewTrip(Long lastTimestamp, long timestamp) {
        return lastTimestamp == null ||
                (timestamp - lastTimestamp) > NEW_TRIP_THRESHOLD_MS;
    }
    /**
     * Initializes state for a new trip.
     * Called when first message arrives or after
     * a gap indicating trip boundary.
     */
    private void startNewTrip(String vin, CarPosition position,
                              long timestamp, Collector<String> out)
            throws Exception {
        tripStartTimestampState.update(timestamp);
        startLatState.update(position.getLocation().getLatitude());
        startLonState.update(position.getLocation().getLongitude());
        totalDistanceState.update(0.0);
        totalReadingsState.update(1);
        maxSpeedState.update(0.0);
        out.collect("New trip started for VIN: " + vin);
    }
    /**
     * Processes continuing trip message.
     * Computes distance and speed via Haversine.
     * Filters speed anomalies (>300 kph = GPS glitch).
     * Generates speed alert if over limit.
     */
    private void continueTrip(String vin, CarPosition position,
                              long timestamp, long lastTimestamp,
                              Collector<String> out) throws Exception {
        double fromLat = lastLatState.value();
        double fromLon = lastLonState.value();
        double toLat = position.getLocation().getLatitude();
        double toLon = position.getLocation().getLongitude();

        double distance = GPSUtil.haversine(fromLat, fromLon, toLat, toLon);
        double speedKph = GPSUtil.computeSpeedKph(distance, lastTimestamp, timestamp);

        if (speedKph > SPEED_ANOMALY_KPH) return;

        totalDistanceState.update(totalDistanceState.value() + distance);
        totalReadingsState.update(totalReadingsState.value() + 1);

        if (speedKph > maxSpeedState.value()) {
            maxSpeedState.update(speedKph);
        }

        if (speedKph > SPEED_LIMIT_KPH) {
            saveSpeedAlert(vin, position, timestamp, speedKph, out);
        }
    }
    /**
     * Updates last known position state.
     * Called after both new trip and continuing trip processing.
     */
    private void updatePosition(CarPosition position, long timestamp)
            throws Exception {
        lastLatState.update(position.getLocation().getLatitude());
        lastLonState.update(position.getLocation().getLongitude());
        lastTimestampState.update(timestamp);
    }
    /**
     * Cancels existing trip timeout timer.
     * Called on every message to reset the timeout.
     */
    private void resetTimer(Context ctx) throws Exception {
        Long existingTimer = timerState.value();
        if (existingTimer != null) {
            ctx.timerService().deleteProcessingTimeTimer(existingTimer);
        }
    }
    /**
     * Registers new trip timeout timer.
     * If no message arrives within TRIP_TIMEOUT_MS,
     * onTimer() fires and completes the trip.
     */
    private void registerTimer(Context ctx) throws Exception {
        long timerTime = ctx.timerService().currentProcessingTime() + TRIP_TIMEOUT_MS;
        ctx.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);
    }
    /**
     * Saves speed alert to MongoDB and notifies downstream.
     */
    private void saveSpeedAlert(String vin, CarPosition position,
                                long timestamp, double speedKph,
                                Collector<String> out) {
        Document alert = new Document()
                .append("vin", vin)
                .append("timestamp", timestamp)
                .append("computedSpeedKph", speedKph)
                .append("latitude", position.getLocation().getLatitude())
                .append("longitude", position.getLocation().getLongitude());
        speedAlertsCollection.insertOne(alert);
        out.collect("SPEED ALERT VIN: " + vin +
                " speed: " + String.format("%.2f", speedKph) + "kph");
    }

    /**
     * This method implements boundary detection via timeout. Every position message resets
     * a 60-seconds processing time timer. If no message arrives within 60 seconds - vehicle
     * went silent, stopped, or lost signal - the timer fires and onTimer() saves the completed trip
     * summary to MongoDB. and clears all the state for that VIN.
     * Flink timer service is fault-tolerant via checkpointing, so even if the job crashes mid-trip,
     * timers are restored and fire correctly after recovery.
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        String vin = ctx.getCurrentKey();

        // grab all state before clearing
        Long tripStart = tripStartTimestampState.value();
        Long lastTimestamp = lastTimestampState.value();
        Double totalDistance = totalDistanceState.value();
        Integer totalReadings = totalReadingsState.value();
        Double maxSpeed = maxSpeedState.value();
        Double startLat = startLatState.value();
        Double startLon = startLonState.value();
        Double lastLat = lastLatState.value();
        Double lastLon = lastLonState.value();
        Double durationSeconds = (lastTimestamp - tripStart) / 1000.0;
        Double avgSpeedKph = durationSeconds > 0 ?
                (totalDistance / durationSeconds) * 3.6
                : 0.0;

        if (tripStart != null) {
            Document trip = new Document()
                    .append("vin", vin)
                    .append("tripStartTimestamp", tripStart)
                    .append("lastUpdateTimestamp", lastTimestamp)
                    .append("totalDistanceMeters", totalDistanceState.value())
                    .append("maxSpeedKph", maxSpeedState.value())
                    .append("totalReadings", totalReadingsState.value())
                    .append("startLat", startLatState.value())
                    .append("startLon", startLonState.value())
                    .append("lastLat", lastLatState.value())
                    .append("lastLon", lastLonState.value())
                    .append("averageSpeedKph", avgSpeedKph);

            completedTripsCollection.insertOne(trip);

            out.collect(String.format(
                    "Trip COMPLETED for VIN: %s | duration: %s | distance: %.2fm | maxSpeed: %.2fkph | readings: %d",
                    vin,
                    GPSUtil.formatDuration(lastTimestamp - tripStart),
                    totalDistance,
                    maxSpeed,
                    totalReadings
            ));
        }

        // clear ALL states
        lastLatState.clear();
        lastLonState.clear();
        lastTimestampState.clear();
        totalDistanceState.clear();
        totalReadingsState.clear();
        tripStartTimestampState.clear();
        startLatState.clear();
        startLonState.clear();
        maxSpeedState.clear();
        timerState.clear();
    }
}