package com.cartelemetry.car_flink.functions;

import com.cartelemetry.car_flink.util.TaggedEvent;
import com.cartelemetry.proto.CarDiagnostics;
import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

class VehicleSnapshotProcessFunctionTest {

    private MongoCollection<Document> snapshotsCollection;
    private ValueState<Double> lastLatState;
    private ValueState<Double> lastLonState;
    private ValueState<Double> lastSpeedState;
    private ValueState<Double> lastHeadingState;
    private ValueState<Long> lastPositionTimestampState;

    private VehicleSnapshotProcessFunction function;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        snapshotsCollection = mock(MongoCollection.class);
        lastLatState = mock(ValueState.class);
        lastLonState = mock(ValueState.class);
        lastSpeedState = mock(ValueState.class);
        lastHeadingState = mock(ValueState.class);
        lastPositionTimestampState = mock(ValueState.class);

        function = new VehicleSnapshotProcessFunction(
                snapshotsCollection);

        injectStates();
    }

    private void injectStates() throws Exception {
        injectField("lastLatState", lastLatState);
        injectField("lastLonState", lastLonState);
        injectField("lastSpeedState", lastSpeedState);
        injectField("lastHeadingState", lastHeadingState);
        injectField("lastPositionTimestampState",
                lastPositionTimestampState);
    }

    private void injectField(String name, Object value)
            throws Exception {
        Field field = VehicleSnapshotProcessFunction.class
                .getDeclaredField(name);
        field.setAccessible(true);
        field.set(function, value);
    }

    // Helper — build TaggedEvent with POSITION:
    private TaggedEvent buildPositionEvent(String vin,
                                           double lat, double lon, double speed) {
        CarPosition pos = CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(System.currentTimeMillis())
                .setSpeed(speed)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .setHeading(0)
                .build();
        return new TaggedEvent("POSITION", pos.toByteArray());
    }

    // Helper — build TaggedEvent with DIAGNOSTICS:
    private TaggedEvent buildDiagnosticsEvent(String vin) {
        CarDiagnostics diag = CarDiagnostics.newBuilder()
                .setVin(vin)
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(180)
                .setFuelLevel(0.5)
                .setRpm(2000)
                .build();
        return new TaggedEvent("DIAGNOSTICS", diag.toByteArray());
    }

    private KeyedProcessFunction<String, TaggedEvent, String>
            .Context mockContext() {
        return mock(KeyedProcessFunction.Context.class);
    }

    @SuppressWarnings("unchecked")
    private Collector<String> mockCollector() {
        return mock(Collector.class);
    }

    @Test
    void testPositionEventUpdatesState() throws Exception {
        // Position event should update state
        // but NOT write to MongoDB
        function.processElement(
                buildPositionEvent("VIN001",
                        30.2672, -97.7431, 50.0),
                mockContext(),
                mockCollector());

        // State updated ✓
        verify(lastLatState).update(30.2672);
        verify(lastLonState).update(-97.7431);
        verify(lastSpeedState).update(50.0);

        // NO MongoDB write ✓
        verify(snapshotsCollection, never())
                .insertOne(any(Document.class));
    }

    @Test
    void testDiagnosticsWithPositionCreatesSnapshot()
            throws Exception {
        // Prior position exists in state:
        when(lastLatState.value()).thenReturn(30.2672);
        when(lastLonState.value()).thenReturn(-97.7431);
        when(lastSpeedState.value()).thenReturn(50.0);
        when(lastHeadingState.value()).thenReturn(0.0);
        when(lastPositionTimestampState.value())
                .thenReturn(System.currentTimeMillis());

        // Diagnostics arrive:
        function.processElement(
                buildDiagnosticsEvent("VIN001"),
                mockContext(),
                mockCollector());

        // Snapshot saved to MongoDB! ✓
        verify(snapshotsCollection, times(1))
                .insertOne(any(Document.class));
    }

    @Test
    void testDiagnosticsWithoutPositionNoSnapshot()
            throws Exception {
        // No prior position — lastLat is null:
        when(lastLatState.value()).thenReturn(null);

        // Diagnostics arrive:
        function.processElement(
                buildDiagnosticsEvent("VIN001"),
                mockContext(),
                mockCollector());

        // NO snapshot — no position yet! ✓
        verify(snapshotsCollection, never())
                .insertOne(any(Document.class));
    }
}