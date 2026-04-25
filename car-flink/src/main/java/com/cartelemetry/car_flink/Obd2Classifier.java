package com.cartelemetry.car_flink;

public class Obd2Classifier {

    public enum Severity { CRITICAL, HIGH, MEDIUM, LOW }

    public static Severity classify(String code) {
        if (code == null || code.isEmpty()) return Severity.LOW;

        if (code.startsWith("C")) return Severity.CRITICAL;  // chassis/brakes
        if (code.startsWith("B")) return Severity.HIGH;      // body/airbags
        if (code.startsWith("P0")) return Severity.HIGH;     // powertrain generic
        if (code.startsWith("P1")) return Severity.MEDIUM;   // powertrain manufacturer
        if (code.startsWith("P2")) return Severity.MEDIUM;   // powertrain generic
        if (code.startsWith("U")) return Severity.MEDIUM;    // network

        return Severity.LOW;
    }
}