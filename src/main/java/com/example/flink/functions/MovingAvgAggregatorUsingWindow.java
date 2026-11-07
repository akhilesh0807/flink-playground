package com.example.flink.functions;

import com.example.flink.dto.MovingAvg;
import com.example.flink.dto.MovingAvgAccumulator;
import com.example.flink.dto.TickOutRecord;
import com.example.flink.dto.TickRecord;
import org.apache.flink.api.common.functions.AggregateFunction;

public class MovingAvgAggregatorUsingWindow implements AggregateFunction<TickRecord, MovingAvgAccumulator, TickOutRecord> {
    @Override
    public MovingAvgAccumulator createAccumulator() {
        return new MovingAvgAccumulator();
    }

    @Override
    public MovingAvgAccumulator add(TickRecord value, MovingAvgAccumulator accumulator) {
        accumulator.sum += value.close();
        accumulator.count += 1L;
        return accumulator;
    }

    @Override
    public TickOutRecord getResult(MovingAvgAccumulator accumulator) {
        double avg = (accumulator.count == 0) ? 0.0 : accumulator.sum / accumulator.count;
        return new TickOutRecord(avg, 0, 0);
    }

    @Override
    public MovingAvgAccumulator merge(MovingAvgAccumulator a, MovingAvgAccumulator b) {
        a.sum += b.sum;
        a.count += b.count;
        return a;
    }
}
