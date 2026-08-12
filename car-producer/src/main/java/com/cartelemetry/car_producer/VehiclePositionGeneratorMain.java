package com.cartelemetry.car_producer;

import com.cartelemetry.car_producer.service.VehiclePositionGenerator;
import com.cartelemetry.car_producer.service.VehicleRegistry;

/**
 * TODO: This is a part of new Vehicle Generating model. Instead of GPS data delivered by bunches of 10
 *  every second, this model spawns a virtual thread for every vin and keeps generating GPS positions
 *  for every vehicle INDEPENDENTLY, still do not have "stoppages" but they will be coming ...
 */
public class VehiclePositionGeneratorMain {
    public static void main(String[] args)
            throws InterruptedException {

        // Create generator directly:
        VehiclePositionGenerator generator =
                new VehiclePositionGenerator(new VehicleRegistry(10), null);

        // Initialize (starts VirtualThreads):
        generator.init();

        // Watch for 30 seconds:
        System.out.println("Generator running... watching for 30s");
        Thread.sleep(30000);

        System.out.println("Done!");
    }
}
