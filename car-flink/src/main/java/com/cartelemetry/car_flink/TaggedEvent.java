package com.cartelemetry.car_flink;

import com.cartelemetry.proto.CarDiagnostics;
import com.cartelemetry.proto.CarPosition;

import java.io.Serializable;
//There are just TWO TaggedEvent types:
// - POSITION
// - DIAGNOSTICS
record TaggedEvent(String type, byte[] data) implements Serializable {
    public String getVin() {
        try {
            if (type.equals("POSITION")) {
                return CarPosition.parseFrom(data).getVin();
            } else {
                return CarDiagnostics.parseFrom(data).getVin();
            }
        } catch (Exception e) {
            return "unknown";
        }
    }

}

