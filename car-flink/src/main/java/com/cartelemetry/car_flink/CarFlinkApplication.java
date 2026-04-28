package com.cartelemetry.car_flink;

import com.cartelemetry.proto.CarDiagnostics;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import com.cartelemetry.proto.CarPosition;

public class CarFlinkApplication {
	public static void main(String[] args) throws Exception {
		String timestamp = String.valueOf(System.currentTimeMillis());
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Flink job will be built here
		String kafkaBootstrap = System.getenv().getOrDefault(
				"KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");

		KafkaSource<byte[]> positionsSource = KafkaSource.<byte[]>builder()
				.setBootstrapServers(kafkaBootstrap)
				.setTopics("car-positions")
				.setGroupId("car-flink-positions-" + timestamp)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
				.build();

		KafkaSource<byte[]> diagnosticsSource = KafkaSource.<byte[]>builder()
				.setBootstrapServers(kafkaBootstrap)
				.setTopics("car-diagnostics")
				.setGroupId("car-flink-diagnostics-" + timestamp)
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
				.build();

		//positions pipeline
		DataStream<String> positionsOutput = env.fromSource(positionsSource, WatermarkStrategy.noWatermarks(), "Kafka Car Positions")
				.keyBy(bytes -> {
					try {
						return CarPosition.parseFrom(bytes).getVin();  // extract VIN for keying
					} catch (Exception e) {
						return "unknown";
					}
				})
				.process(new CarPositionProcessFunction());  // deserialize inside processElement

		//diagnostics pipeline
		DataStream<String> diagnosticsOutput = env.fromSource(diagnosticsSource, WatermarkStrategy.noWatermarks(), "Kafka Car Diagnostics")
				.keyBy(bytes -> {
					try {
						return CarDiagnostics.parseFrom(bytes).getVin();  // extract VIN for keying
					} catch (Exception e) {
						return "unknown";
					}
				})
				.process(new CarDiagnosticsProcessFunction());  // deserialize inside processElement

		//BOTH streams also feed VehicleSnapshotProcessFunction
		DataStream<TaggedEvent> unifiedStream =
				env.fromSource(positionsSource, WatermarkStrategy.noWatermarks(), "Positions for Snapshot")
						.map(bytes -> new TaggedEvent("POSITION", bytes))
						.union(
								env.fromSource(diagnosticsSource, WatermarkStrategy.noWatermarks(), "Diagnostics for Snapshot")
										.map(bytes -> new TaggedEvent("DIAGNOSTICS", bytes)));
		unifiedStream
				.keyBy(TaggedEvent::getVin)
				.process(new VehicleSnapshotProcessFunction());
		env.execute("Car Telemetry Flink Job");
	}
}