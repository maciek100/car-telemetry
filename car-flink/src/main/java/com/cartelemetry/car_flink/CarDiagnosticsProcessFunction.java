package com.cartelemetry.car_flink;

import com.cartelemetry.proto.CarDiagnostics;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;

public class CarDiagnosticsProcessFunction
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
    private transient MongoCollection<Document> diagnosticsAlertsCollection;
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(CarDiagnosticsProcessFunction.class);
    @Override
    public void open(OpenContext openContext) throws Exception {
        String mongoUri = System.getenv().getOrDefault(
                "MONGODB_URI", "mongodb://localhost:27017");
        //MongoClient mongoClient = MongoClients.create(mongoUri);
        //MongoDatabase db = mongoClient.getDatabase("cartelemetry");
        diagnosticsAlertsCollection = MongoClients.create(mongoUri)
                .getDatabase("cartelemetry")
                .getCollection("flink_diagnostics_alerts");
    }

    ///private static final long TRIP_TIMEOUT_MS = 60 * 1000;  // 5 minutes
    ///private static final double SPEED_LIMIT_KPH = 120.0;
    ///private static final double SPEED_ANOMALY_KPH = 300.0;
    ///private long flinkStartTime;


    @Override
    public void processElement(byte[] value, Context ctx,
                               Collector<String> out) throws Exception {
        log.info("!!! DIAGNOSTICS RECEIVED !!!");
        CarDiagnostics diag = CarDiagnostics.parseFrom(value);
        log.info("VIN: {}", diag.getVin());
        if (diag.getEngineTemp() > 210) {
            saveAlert(diag, "HIGH_ENGINE_TEMP",
                    "Engine temp: " + diag.getEngineTemp());
        }
        if (diag.getFuelLevel() < 0.1) {
            saveAlert(diag, "LOW_FUEL",
                    "Fuel level: " + diag.getFuelLevel());
        }
        if (!diag.getObd2ErrorCodesList().isEmpty()) {
            saveAlert(diag, "OBD2_ERROR",
                    "Codes: " + diag.getObd2ErrorCodesList());
        }
    }

    private void saveAlert(CarDiagnostics diag, String alertType, String message) {
        Document alert = new Document()
                .append("vin", diag.getVin())
                .append("timestamp", diag.getTimestamp())
                .append("alertType", alertType)
                .append("message", message)
                .append("engineTemp", diag.getEngineTemp())
                .append("fuelLevel", diag.getFuelLevel())
                .append("obd2ErrorCodes", diag.getObd2ErrorCodesList());
        diagnosticsAlertsCollection.insertOne(alert);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        // no timers needed for diagnostics
    }
}