package com.cartelemetry.car_flink;

import com.cartelemetry.proto.CarPosition;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class CarPositionProcessFunction
        extends KeyedProcessFunction<String, byte [], String> {


//    public void processElement(CarPosition value, Context ctx,
//                               Collector<String> out) throws Exception {
//        out.collect("VIN: " + value.getVin() +
//                " timestamp: " + value.getTimestamp());
//    }

    @Override
    public void processElement(byte[] value, Context ctx,
                               Collector<String> out) throws Exception {
        CarPosition position = CarPosition.parseFrom(value);
        out.collect("VIN: " + position.getVin() +
                " timestamp: " + position.getTimestamp());
    }
}