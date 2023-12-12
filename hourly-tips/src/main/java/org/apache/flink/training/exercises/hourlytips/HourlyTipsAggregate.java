package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

public class HourlyTipsAggregate implements AggregateFunction<TaxiFare, Tuple3<Long, Long, Float>, Tuple3<Long, Long, Float>> {
    @Override
    public Tuple3<Long, Long, Float> createAccumulator() {
        return new Tuple3<>(0L, 0L, 0F);
    }

    @Override
    public Tuple3<Long, Long, Float> add(TaxiFare value, Tuple3<Long, Long, Float> accumulator) {
        return new Tuple3<>(value.startTime.toEpochMilli(), value.driverId, accumulator.f2 + value.tip);
    }

    @Override
    public Tuple3<Long, Long, Float> getResult(Tuple3<Long, Long, Float> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple3<Long, Long, Float> merge(Tuple3<Long, Long, Float> a, Tuple3<Long, Long, Float> b) {
        return new Tuple3<>(a.f0, a.f1, a.f2 + b.f2);
    }
}
