package com.cartelemetry.car_flink.functions;

import com.cartelemetry.car_flink.util.MongoUtil;
import com.cartelemetry.car_flink.util.Obd2Classifier;
import com.cartelemetry.proto.CarDiagnostics;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CarDiagnosticsProcessFunction
        extends KeyedProcessFunction<String, byte[], String> {

    private /*transient*/ MongoCollection<Document> diagnosticsAlertsCollection;
    private /*transient*/ MongoCollection<Document> criticalWarningCollection;

    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(CarDiagnosticsProcessFunction.class);

    private ValueState<Integer> alertCounterState;
    private final int alertThreshold = 5;

    private final String diagnosticsCollectionName;
    private final String criticalWarningCollectionName;

    //Production constructor
    public CarDiagnosticsProcessFunction() {
        this("flink_diagnostics_alerts","flink_critical_warnings");
    }
    //Test constructor -injectable collection names
    CarDiagnosticsProcessFunction(String diagnosticsCollectionName, String criticalWarningCollectionName) {
        this.diagnosticsCollectionName = diagnosticsCollectionName;
        this.criticalWarningCollectionName = criticalWarningCollectionName;
    }
    //Test constructor - with actual collections ijested

    public CarDiagnosticsProcessFunction(MongoCollection<Document> diagnosticsCollection,
                                         MongoCollection<Document> criticalWarningsCollection) {
        this.diagnosticsCollectionName = null;
        this.criticalWarningCollectionName = null;
        this.diagnosticsAlertsCollection = diagnosticsCollection;
        this.criticalWarningCollection = criticalWarningsCollection;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        if (diagnosticsAlertsCollection == null) {
            diagnosticsAlertsCollection = MongoUtil.getCollection(diagnosticsCollectionName);
            criticalWarningCollection = MongoUtil.getCollection(criticalWarningCollectionName);
        }
        alertCounterState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("alertCount", Integer.class));
    }

    @Override
    public void processElement(byte[] value, Context ctx,
                               Collector<String> out) throws Exception {
        CarDiagnostics diag = CarDiagnostics.parseFrom(value);
        if (diag.getEngineTemp() > 210) {
            recordAlert(diag, "HIGH_ENGINE_TEMP",
                    "Engine temp: " + diag.getEngineTemp());
        }
        if (diag.getFuelLevel() < 0.1) {
            recordAlert(diag, "LOW_FUEL",
                    "Fuel level: " + diag.getFuelLevel());
        }
        for (String code : diag.getObd2ErrorCodesList()) {
            Obd2Classifier.Severity severity = Obd2Classifier.classify(code);
            recordAlert(diag, "OBD2_" + severity.name(), "Code: " + code + " Severity: " + severity);
        }
    }

    private void recordAlert(CarDiagnostics diag, String alertType, String message) throws Exception {
        Document alert = new Document()
                .append("vin", diag.getVin())
                .append("timestamp", diag.getTimestamp())
                .append("alertType", alertType)
                .append("message", message)
                .append("engineTemp", diag.getEngineTemp())
                .append("fuelLevel", diag.getFuelLevel());
        diagnosticsAlertsCollection.insertOne(alert);
        //increment alert count for VIN
        Integer count = alertCounterState.value();
        count = (count == null) ? 1 : count + 1;
        alertCounterState.update(count);

        //check if threshold reached ...
        if (count > alertThreshold) {
            Document warning = new Document()
                    .append("vin", diag.getVin())
                    .append("timestamp", diag.getTimestamp())
                    .append("alertCount", count)
                    .append("message", "Vehicle" + diag.getVin() + " has triggered " + count + " alerts!");
            criticalWarningCollection.insertOne(warning);
            log.info("CRITICAL WARNING for VIN: " + diag.getVin() + " alerts: " + count);
            alertCounterState.update(0);
        }

    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<String> out) throws Exception {
        // no timers needed for diagnostics
    }
}