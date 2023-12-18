package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;

import java.time.Duration;

public class TipAggregate implements AggregateFunction<TaxiFare, Tuple2<Long, Float>, Tuple2<Long, Float>> {
        @Override
        public Tuple2<Long, Float> createAccumulator() {
            return new Tuple2<>(0L, 0F);
        }

        @Override
        public Tuple2<Long, Float> add(TaxiFare fare, Tuple2<Long, Float> accumulator) {

            return new Tuple2<>(fare.driverId, accumulator.f1 + fare.tip);
        }

        @Override
        public Tuple2<Long, Float> getResult(Tuple2<Long, Float> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<Long, Float> merge(Tuple2<Long, Float> a, Tuple2<Long, Float> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }


