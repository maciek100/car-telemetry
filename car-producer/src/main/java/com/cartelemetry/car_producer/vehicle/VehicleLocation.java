package com.cartelemetry.car_producer.vehicle;

import java.time.Instant;

public  record VehicleLocation(
        String vin,
        double latitude,
        double longitude,
        double heading,
        double currentSpeed,
        boolean stopped,
        Instant stopUntil) {
    VehicleLocation withNewPosition(String vin, double newLat, double newLon, double newHeading, double newSpeed) {
        return new VehicleLocation(vin, newLat, newLon, newHeading, newSpeed, false, null);
    }
    VehicleLocation withStopped(Instant stopUntil) {
        return new VehicleLocation(this.vin, this.latitude, this.longitude, this.heading, this.currentSpeed, true, stopUntil);
    }
    VehicleLocation withMoving() {
        return new VehicleLocation(this.vin, this.latitude, this.longitude, this.heading, this.currentSpeed, false, null);
    }
}

