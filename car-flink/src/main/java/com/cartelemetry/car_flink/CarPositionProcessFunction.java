package com.cartelemetry.car_flink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.cartelemetry.proto.CarPosition;
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

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> completedTripsCollection;
    private transient MongoCollection<Document> speedAlertsCollection;

    @Override
    public void open(OpenContext openContext) throws Exception {
        flinkStartTime = System.currentTimeMillis() + 15000;

        String mongoUri = System.getenv().getOrDefault(
                "MONGODB_URI", "mongodb://localhost:27017");
        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase db = mongoClient.getDatabase("cartelemetry");
        completedTripsCollection = db.getCollection("flink_completed_trips");
        speedAlertsCollection = db.getCollection("flink_speed_alerts");

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

    private static final long TRIP_TIMEOUT_MS = 60 * 1000;  // 5 minutes
    private static final double SPEED_LIMIT_KPH = 120.0;
    private static final double SPEED_ANOMALY_KPH = 300.0;
    private long flinkStartTime;


    @Override
    public void processElement(byte[] value, Context ctx,
                               Collector<String> out) throws Exception {

        CarPosition position = CarPosition.parseFrom(value);
        long timestamp = position.getTimestamp();
        long cutoff = timestamp - 10000; // 10 seconds ago
        List<Long> toRemove = new ArrayList<>();
        for (Long ts : seenTimeStamps.keys()) {
            if (ts < cutoff) {
                toRemove.add(ts);
            }
        }
        for (Long ts : toRemove) {
            seenTimeStamps.remove(ts);
        }
        if (seenTimeStamps.contains(position.getTimestamp())) {
            return;
        }
        seenTimeStamps.put(position.getTimestamp(), true);
        String vin = position.getVin();

        if (timestamp < flinkStartTime) {
            out.collect("ARRIVED TOO EARLY: " + vin +
                            " taken : " + (timestamp - flinkStartTime));
            return;
        }


        // cancel existing timer
        Long existingTimer = timerState.value();
        if (existingTimer != null) {
            ctx.timerService().deleteProcessingTimeTimer(existingTimer);
        }

        // is this a new trip?
        Long lastTimestamp = lastTimestampState.value();
        if (lastTimestamp == null) {
            if (lastTimestamp != null && (timestamp - lastTimestamp) < 500) {
                return;
            }
            // NEW TRIP!
            tripStartTimestampState.update(timestamp);
            startLatState.update(position.getLocation().getLatitude());
            startLonState.update(position.getLocation().getLongitude());
            totalDistanceState.update(0.0);
            totalReadingsState.update(1);
            maxSpeedState.update(0.0);
            out.collect("New trip started for VIN: " + vin);
        } else {
            // CONTINUING TRIP
            long timeDelta = timestamp - lastTimestamp;
            if (timeDelta < 500) {
                lastLatState.update(position.getLocation().getLatitude());
                lastLonState.update(position.getLocation().getLongitude());
                lastTimestampState.update(timestamp);
            }
            double fromLat = lastLatState.value();
            double fromLon = lastLonState.value();
            double toLat = position.getLocation().getLatitude();
            double toLon = position.getLocation().getLongitude();

//            out.collect("DEBUG VIN: " + vin +
//                    " timeDelta: " + timeDelta + "ms" +
//                    " fromLat: " + fromLat + " toLat: " + toLat +
//                    " fromLon: " + fromLon + " toLon: " + toLon);
            double distance = haversine(fromLat, fromLon, toLat, toLon);
            double speedKph = computeSpeedKph(distance, lastTimestamp, timestamp);

            if (speedKph > SPEED_ANOMALY_KPH) {


                out.collect("ANOMALY DEBUG VIN: " + vin +
                        " speed: " + String.format("%.2f", speedKph) +
                        " distance: " + String.format("%.2f", distance) + "m" +
                        " timeDelta: " + (timestamp - lastTimestamp) + "ms" +
                        " fromLat: " + fromLat + " fromLon: " + fromLon +
                        " toLat: " + toLat + " toLon: " + toLon);
                //out.collect("GPS anomaly for VIN: " + vin +
                //        " speed: " + String.format("%.2f", speedKph) + "kph");
            } else {
                // update state
                double newDistance = totalDistanceState.value() + distance;
                totalDistanceState.update(newDistance);
                totalReadingsState.update(totalReadingsState.value() + 1);

                if (speedKph > maxSpeedState.value()) {
                    maxSpeedState.update(speedKph);
                }

                if (speedKph > SPEED_LIMIT_KPH) {
                    Document speedAlert = new Document()
                            .append("vin", vin)
                            .append("timestamp", timestamp)
                            .append("computedSpeedKph", speedKph)
                            .append("latitude", position.getLocation().getLatitude())
                            .append("longitude", position.getLocation().getLongitude());

                    speedAlertsCollection.insertOne(speedAlert);
                    out.collect("SPEED ALERT VIN: " + vin +
                            " speed: " + String.format("%.2f", speedKph) + "kph");
                }
            }
        }

        // update last position
        lastLatState.update(position.getLocation().getLatitude());
        lastLonState.update(position.getLocation().getLongitude());
        lastTimestampState.update(timestamp);

        // set new timeout timer
        long timerTime = ctx.timerService().currentProcessingTime() + TRIP_TIMEOUT_MS;
        ctx.timerService().registerProcessingTimeTimer(timerTime);
        timerState.update(timerTime);
    }

    private double haversine(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371000;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
                Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
                        Math.sin(dLon/2) * Math.sin(dLon/2);
        return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    }

    double computeSpeedKph(double distanceMeters, long fromTs, long toTs) {
        double seconds = (toTs - fromTs) / 1000.0;
        if (seconds <= 0) return 0;
        return (distanceMeters / seconds) * 3.6;
    }

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
                    formatDuration(lastTimestamp - tripStart),
                    totalDistance,
                    maxSpeed,
                    totalReadings
            ));
        }

        // clear ALL state
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

    private String formatDuration(long millis) {
        long minutes = millis / 60000;
        long seconds = (millis % 60000) / 1000;
        return String.format("%dm%02ds", minutes, seconds);
    }
}