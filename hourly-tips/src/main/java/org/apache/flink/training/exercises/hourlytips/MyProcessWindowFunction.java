package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<Long, Float>, Tuple3<Long, Long, Float>, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Tuple2<Long, Float>> fareByDriverIDs, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
        for (Tuple2<Long, Float> element : fareByDriverIDs) {
            out.collect(new Tuple3<Long, Long, Float>(context.window().getEnd(), element.f0, element.f1));
        }
    }
}

