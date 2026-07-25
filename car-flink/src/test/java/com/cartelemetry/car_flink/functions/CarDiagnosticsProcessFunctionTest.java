package com.cartelemetry.car_flink.functions;

import com.cartelemetry.proto.CarDiagnostics;
import com.mongodb.client.MongoCollection;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CarDiagnosticsProcessFunctionTest {

    private MongoCollection<Document> diagnosticsCollection;
    private MongoCollection<Document> criticalWarningsCollection;
    private RuntimeContext runtimeContext;
    private ValueState<Integer> alertCounterState;
    private CarDiagnosticsProcessFunction function;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        diagnosticsCollection = mock(MongoCollection.class);
        criticalWarningsCollection = mock(MongoCollection.class);
        alertCounterState = mock(ValueState.class);
        runtimeContext = mock(RuntimeContext.class);

        function = new CarDiagnosticsProcessFunction(
                diagnosticsCollection,
                criticalWarningsCollection);

        function.setRuntimeContext(runtimeContext);

        Field stateField = CarDiagnosticsProcessFunction.class
                .getDeclaredField("alertCounterState");
        stateField.setAccessible(true);
        stateField.set(function, alertCounterState);
    }

    private KeyedProcessFunction<String, byte[], String>.Context mockContext() {
        return mock(KeyedProcessFunction.Context.class);
    }

    private Collector<String> mockCollector() {
        return mock(Collector.class);
    }

    @Test
    void testCriticalWarningAfterThreshold() throws Exception {
        // Simulate 5 previous alerts
        when(alertCounterState.value()).thenReturn(5);

        CarDiagnostics diag = CarDiagnostics.newBuilder()
                .setVin("VIN_TEST_001")
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(215)  // triggers alert
                .setFuelLevel(0.5)
                .build();

        function.processElement(
                diag.toByteArray(),
                mockContext(),
                mockCollector());

        // Regular alert inserted ✓
        verify(diagnosticsCollection, times(1))
                .insertOne(any(Document.class));

        // Critical warning inserted! ✓
        verify(criticalWarningsCollection, times(1))
                .insertOne(any(Document.class));

        // Counter reset to 0 ✓
        verify(alertCounterState, times(1)).update(0);
    }

    @Test
    void testHighEngineTempAlert() throws Exception {
        // Build diagnostics with HIGH engine temp
        CarDiagnostics diag = CarDiagnostics.newBuilder()
                .setVin("VIN_TEST_001")
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(215)  // > 210 threshold!
                .setFuelLevel(0.5)   // normal
                .build();

        // Call processElement
        function.processElement(
                diag.toByteArray(),
                mockContext(),
                mockCollector());

        // Verify alert inserted into MongoDB!
        verify(diagnosticsCollection, times(1))
                .insertOne(any(Document.class));
    }

    @Test
    void testNormalDiagnosticsNoAlert() throws Exception {
        CarDiagnostics diag = CarDiagnostics.newBuilder()
                .setVin("VIN_TEST_001")
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(180)  // normal < 210
                .setFuelLevel(0.5)   // normal > 0.1
                .build();

        function.processElement(
                diag.toByteArray(),
                mockContext(),
                mockCollector());

        // NO alert should be inserted!
        verify(diagnosticsCollection, never())
                .insertOne(any(Document.class));
    }

    @Test
    void testLowFuelAlert() throws Exception {
        CarDiagnostics diag = CarDiagnostics.newBuilder()
                .setVin("VIN_TEST_001")
                .setTimestamp(System.currentTimeMillis())
                .setEngineTemp(180)   // normal
                .setFuelLevel(0.05)   // < 0.1 threshold!
                .build();

        function.processElement(
                diag.toByteArray(),
                mockContext(),
                mockCollector());

        verify(diagnosticsCollection, times(1))
                .insertOne(any(Document.class));
    }
}