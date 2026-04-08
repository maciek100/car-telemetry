package com.cartelemetry.car_consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class CarConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(CarConsumerApplication.class, args);
	}

}
