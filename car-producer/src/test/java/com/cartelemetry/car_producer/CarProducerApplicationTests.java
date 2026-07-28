package com.cartelemetry.car_producer;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootTest
class CarProducerApplicationTests {

	@MockBean
	KafkaTemplate<String, byte[]> kafkaTemplate;

	@Test
	void contextLoads() {
	}

}
