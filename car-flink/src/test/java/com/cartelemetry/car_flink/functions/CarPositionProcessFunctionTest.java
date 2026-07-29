package com.cartelemetry.car_flink.functions;

import com.cartelemetry.proto.CarPosition;
import com.cartelemetry.proto.GpsLocation;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

public class CarPositionProcessFunctionTest {

/**
 * TESTS NEEDED :
 * → New trip detection
 * → Trip continuation
 * → Distance calculation
 * → Speed alert generation
 * → Duplicate detection
 * → Startup cutoff (flinkStartTime)
 * → Speed anomaly filtering
 * → Trip completion (onTimer)
 */
    private MongoCollection<Document> completedTripsCollection;
    private MongoCollection<Document> speedAlertsCollection;
    // All 11 states:
    private ValueState<Double> lastLatState;
    private ValueState<Double> lastLonState;
    private ValueState<Long> lastTimestampState;
    private ValueState<Double> totalDistanceState;
    private ValueState<Integer> totalReadingsState;
    private ValueState<Long> tripStartTimestampState;
    private ValueState<Double> startLatState;
    private ValueState<Double> startLonState;
    private ValueState<Double> maxSpeedState;
    private ValueState<Long> timerState;
    private MapState<Long, Boolean> seenTimeStamps;

    private CarPositionProcessFunction function;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        // Initialize mocks:
        completedTripsCollection = mock(MongoCollection.class);
        speedAlertsCollection = mock(MongoCollection.class);
        lastLatState = mock(ValueState.class);
        lastLonState = mock(ValueState.class);
        lastTimestampState = mock(ValueState.class);
        totalDistanceState = mock(ValueState.class);
        totalReadingsState = mock(ValueState.class);
        tripStartTimestampState = mock(ValueState.class);
        startLatState = mock(ValueState.class);
        startLonState = mock(ValueState.class);
        maxSpeedState = mock(ValueState.class);
        timerState = mock(ValueState.class);
        seenTimeStamps = mock(MapState.class);

        // Create function with injected collections:
        function = new CarPositionProcessFunction(
                completedTripsCollection,
                speedAlertsCollection);

        // Inject all states via helper:
        injectStates();
    }

    // Helper — inject all states via reflection:
    @SuppressWarnings("unchecked")
    private void injectStates() throws Exception {
        injectField("lastLatState", lastLatState);
        injectField("lastLonState", lastLonState);
        injectField("lastTimestampState", lastTimestampState);
        injectField("totalDistanceState", totalDistanceState);
        injectField("totalReadingsState", totalReadingsState);
        injectField("tripStartTimestampState", tripStartTimestampState);
        injectField("startLatState", startLatState);
        injectField("startLonState", startLonState);
        injectField("maxSpeedState", maxSpeedState);
        injectField("timerState", timerState);
        injectField("seenTimeStamps", seenTimeStamps);
        // Also set flinkStartTime to past so messages aren't filtered:
        injectField("flinkStartTime", 0L);
    }

    private void injectField(String name, Object value)
            throws Exception {
        Field field = CarPositionProcessFunction.class
                .getDeclaredField(name);
        field.setAccessible(true);
        field.set(function, value);
    }

    // Mock helpers:
    private KeyedProcessFunction<String, byte[], String>.Context mockContext() {
        //return mock(KeyedProcessFunction.Context.class);
        KeyedProcessFunction<String, byte[], String>
                .Context ctx = mock(
                KeyedProcessFunction.Context.class);

        // Stub timerService!
        TimerService timerService = mock(TimerService.class);
        when(ctx.timerService()).thenReturn(timerService);
        when(timerService.currentProcessingTime())
                .thenReturn(System.currentTimeMillis());

        return ctx;
    }

    @SuppressWarnings("unchecked")
    private Collector<String> mockCollector() {
        return mock(Collector.class);
    }

    // Helper — build CarPosition message:
    private byte[] buildPosition(String vin,
                                 double lat, double lon,
                                 long timestamp) {
        return CarPosition.newBuilder()
                .setVin(vin)
                .setTimestamp(timestamp)
                .setSpeed(50.0)
                .setLocation(GpsLocation.newBuilder()
                        .setLatitude(lat)
                        .setLongitude(lon)
                        .build())
                .setHeading(0)
                .build()
                .toByteArray();
    }

    @Test
    void testNewTripDetection() throws Exception {
        // First message ever for this VIN
        when(lastTimestampState.value()).thenReturn(null);
        when(seenTimeStamps.contains(anyLong())).thenReturn(false);
        when(maxSpeedState.value()).thenReturn(0.0);

        Collector<String> collector = mockCollector();

        function.processElement(
                buildPosition("VIN001", 30.2672, -97.7431,
                        System.currentTimeMillis()),
                mockContext(),
                collector);

        // Verify new trip announced!
        verify(collector).collect(
                contains("New trip started"));
    }
    @Test
    void testSpeedAlert() throws Exception {
        // Vehicle already has a previous position (continuing trip)
        long now = System.currentTimeMillis();
        long oneSecondAgo = now - 1000;

        when(lastTimestampState.value()).thenReturn(oneSecondAgo);
        when(lastLatState.value()).thenReturn(30.2672);
        when(lastLonState.value()).thenReturn(-97.7431);
        when(seenTimeStamps.contains(anyLong())).thenReturn(false);
        when(totalDistanceState.value()).thenReturn(0.0);
        when(totalReadingsState.value()).thenReturn(1);
        when(maxSpeedState.value()).thenReturn(0.0);

        // New position FAR away = high speed!
        // 0.00040 degrees in 1 second = ~160 kph
        function.processElement(
                buildPosition("VIN001",
                        30.2672 + 0.00040,  // far away!
                        -97.7431,
                        now),
                mockContext(),
                mockCollector());

        // Speed alert should be inserted!
        verify(speedAlertsCollection, times(1))
                .insertOne(any(Document.class));
    }

    @Test
    void testNoSpeedAlertNormalSpeed() throws Exception {
        long now = System.currentTimeMillis();
        long oneSecondAgo = now - 1000;

        when(lastTimestampState.value()).thenReturn(oneSecondAgo);
        when(lastLatState.value()).thenReturn(30.2672);
        when(lastLonState.value()).thenReturn(-97.7431);
        when(seenTimeStamps.contains(anyLong())).thenReturn(false);
        when(totalDistanceState.value()).thenReturn(0.0);
        when(totalReadingsState.value()).thenReturn(1);
        when(maxSpeedState.value()).thenReturn(0.0);

        // Close position = slow speed ~48 kph
        function.processElement(
                buildPosition("VIN001",
                        30.2672 + 0.00012,  // close = slow ✓
                        -97.7431,
                        now),
                mockContext(),
                mockCollector());

        // NO speed alert!
        verify(speedAlertsCollection, never())
                .insertOne(any(Document.class));
    }

    @Test
    void testDuplicateMessageIgnored() throws Exception {
        long timestamp = System.currentTimeMillis();

        // Simulate: this timestamp already seen!
        when(seenTimeStamps.contains(timestamp))
                .thenReturn(true);

        Collector<String> collector = mockCollector();

        function.processElement(
                buildPosition("VIN001", 30.2672, -97.7431,
                        timestamp),
                mockContext(),
                collector);

        // Nothing should happen — duplicate ignored!
        verify(speedAlertsCollection, never())
                .insertOne(any(Document.class));
        verify(collector, never())
                .collect(anyString());
    }

    @Test
    void testSpeedAnomalyIgnored() throws Exception {
        long now = System.currentTimeMillis();
        long oneSecondAgo = now - 1000;

        when(lastTimestampState.value()).thenReturn(oneSecondAgo);
        when(lastLatState.value()).thenReturn(30.2672);
        when(lastLonState.value()).thenReturn(-97.7431);
        when(seenTimeStamps.contains(anyLong())).thenReturn(false);
        when(totalDistanceState.value()).thenReturn(0.0);
        when(totalReadingsState.value()).thenReturn(1);
        when(maxSpeedState.value()).thenReturn(0.0);

        // IMPOSSIBLY far = > 300 kph anomaly
        // 0.00400 degrees in 1 second = ~1600 kph! 😱
        function.processElement(
                buildPosition("VIN001",
                        30.2672 + 0.00400,  // impossible distance!
                        -97.7431,
                        now),
                mockContext(),
                mockCollector());

        // Anomaly silently ignored!
        // No speed alert despite huge distance:
        verify(speedAlertsCollection, never())
                .insertOne(any(Document.class));
    }

    @Test
    void testStartupCutoffIgnoresOldMessages() throws Exception {
        // Set flinkStartTime to FUTURE
        // so all messages appear "old"
        injectField("flinkStartTime",
                System.currentTimeMillis() + 60000); // 1 minute future

        when(seenTimeStamps.contains(anyLong()))
                .thenReturn(false);

        Collector<String> collector = mockCollector();

        // Message with current timestamp
        // but flinkStartTime is in future
        // so message appears "stale"
        function.processElement(
                buildPosition("VIN001", 30.2672, -97.7431,
                        System.currentTimeMillis()),
                mockContext(),
                collector);

        // Nothing processed — message too old!
        verify(collector, never())
                .collect(anyString());
        verify(speedAlertsCollection, never())
                .insertOne(any(Document.class));
    }

    @Test
    void testTripCompletionOnTimer() throws Exception {
        long tripStart = System.currentTimeMillis() - 60000;
        long lastUpdate = System.currentTimeMillis() - 1000;

        // Setup trip state:
        when(tripStartTimestampState.value()).thenReturn(tripStart);
        when(lastTimestampState.value()).thenReturn(lastUpdate);
        when(totalDistanceState.value()).thenReturn(500.0);
        when(totalReadingsState.value()).thenReturn(50);
        when(maxSpeedState.value()).thenReturn(75.0);
        when(startLatState.value()).thenReturn(30.2672);
        when(startLonState.value()).thenReturn(-97.7431);
        when(lastLatState.value()).thenReturn(30.2700);
        when(lastLonState.value()).thenReturn(-97.7400);

        // Fire the timer!
        KeyedProcessFunction<String, byte[], String>
                .OnTimerContext timerCtx =
                mock(KeyedProcessFunction.OnTimerContext.class);
        when(timerCtx.getCurrentKey()).thenReturn("VIN001");

        Collector<String> collector = mockCollector();

        function.onTimer(
                System.currentTimeMillis(),
                timerCtx,
                collector);

        // Trip saved to MongoDB!
        verify(completedTripsCollection, times(1))
                .insertOne(any(Document.class));

        // Output collected!
        verify(collector, times(1))
                .collect(argThat(s -> s.contains("Trip COMPLETED")));

        // State cleared!
        verify(lastLatState).clear();
        verify(lastLonState).clear();
        verify(lastTimestampState).clear();
        verify(totalDistanceState).clear();
        verify(tripStartTimestampState).clear();
    }
}
