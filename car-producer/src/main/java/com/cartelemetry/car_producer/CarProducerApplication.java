package com.cartelemetry.car_producer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CarProducerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CarProducerApplication.class, args);
	}

}
