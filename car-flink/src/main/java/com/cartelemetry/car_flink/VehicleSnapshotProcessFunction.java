package com.cartelemetry.car_flink;

import com.cartelemetry.proto.CarDiagnostics;
import com.cartelemetry.proto.CarPosition;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VehicleSnapshotProcessFunction extends KeyedProcessFunction<String, TaggedEvent, String> {

    private static final Logger log = LoggerFactory.getLogger(VehicleSnapshotProcessFunction.class);
    //last state per vehicle
    private ValueState<Double> lastLatState;
    private ValueState<Double> lastLonState;
    private ValueState<Double> lastSpeedState;
    private ValueState<Double> lastHeadingState;
    private ValueState<Long> lastPositionTimestampState;

    private transient MongoCollection<Document> snapshotsCollection;

    @Override
    public void open(OpenContext openContext) throws Exception {
        String mongoUri = System.getenv().getOrDefault(
                "MONGODB_URI", "MONGODB://LOCALHOST:27017");

        snapshotsCollection = MongoClients.create(mongoUri)
                .getDatabase("cartelemetry")
                .getCollection("flink_vehicle_snapshot");
        lastLatState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("snapLastLat", Double.class));
        lastLonState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("snapLastLon", Double.class));
        lastSpeedState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("snapLastSpeed", Double.class));
        lastHeadingState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("snapLastHeading", Double.class));
        lastPositionTimestampState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("snapLastTimestamp", Long.class));
    }

    @Override
    public void processElement(TaggedEvent event, Context ctx, Collector<String> out) throws Exception {
        if (event.type().equals("POSITION")) {
            // just update last position state
            CarPosition pos = CarPosition.parseFrom(event.data());
            lastLatState.update(pos.getLocation().getLatitude());
            lastLonState.update(pos.getLocation().getLongitude());
            lastSpeedState.update(pos.getSpeed());
            lastHeadingState.update(pos.getHeading());
            lastPositionTimestampState.update(pos.getTimestamp());

        } else if (event.type().equals("DIAGNOSTICS")) {
            // diagnostics arrived → combine with last position → save snapshot to the db
            CarDiagnostics diag = CarDiagnostics.parseFrom(event.data());

            if (lastLatState.value() == null) return;  // no position yet

            Document snapshot = new Document()
                    .append("vin", diag.getVin())
                    .append("timestamp", diag.getTimestamp())
                    .append("latitude", lastLatState.value())
                    .append("longitude", lastLonState.value())
                    .append("speed", lastSpeedState.value())
                    .append("heading", lastHeadingState.value())
                    .append("positionTimestamp", lastPositionTimestampState.value())
                    .append("engineTemp", diag.getEngineTemp())
                    .append("fuelLevel", diag.getFuelLevel())
                    .append("rpm", diag.getRpm())
                    .append("batteryVoltage", diag.getBatteryVoltage())
                    .append("oilPressure", diag.getOilPressure())
                    .append("odometer", diag.getOdometer())
                    .append("tirePressureFL", diag.getTirePressureFL())
                    .append("tirePressureFR", diag.getTirePressureFR())
                    .append("tirePressureRL", diag.getTirePressureRL())
                    .append("tirePressureRR", diag.getTirePressureRR())
                    .append("obd2ErrorCodes", diag.getObd2ErrorCodesList());

            snapshotsCollection.insertOne(snapshot);
            log.info("Snapshot saved for VIN : ", diag.getVin());
            out.collect("Snapshot saved for VIN: " + diag.getVin());
        }
    }


}
