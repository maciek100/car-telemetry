package com.cartelemetry.car_flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import com.cartelemetry.proto.CarPosition;

public class CarFlinkApplication {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Flink job will be built here
		KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
				.setBootstrapServers("localhost:29092")
				.setTopics("car-positions")
				.setGroupId("car-flink-group-" + System.currentTimeMillis())
				.setStartingOffsets(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(new ByteArrayDeserializationSchema())
				.build();
/*
		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Car Positions")
				.map(bytes -> CarPosition.parseFrom(bytes))  // deserialize Protobuf
				.keyBy(CarPosition::getVin)                  // key by VIN
				.process(new CarPositionProcessFunction())   // stateful processing
				.print();
*/
		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Car Positions")
				.keyBy(bytes -> {
					try {
						return CarPosition.parseFrom(bytes).getVin();  // extract VIN for keying
					} catch (Exception e) {
						return "unknown";
					}
				})
				.process(new CarPositionProcessFunction())  // deserialize inside processElement
				.print();

		env.execute("Car Telemetry Flink Job");
	}
}