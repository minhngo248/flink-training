/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.common.utils.MissingSolutionException;

import java.time.Duration;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source);

        // Tumbling window of 1 hour
        // Key by driverId
        // hour-long windows that compute the total tips for each driver during the hour
        // and emit a stream of (windowEndTime, driverId, totalTips).
        DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TaxiFare>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMillis() + Duration.ofHours(1).toMillis())
                )
                .map(
                        new MapFunction<TaxiFare, Tuple3<Long, Long, Float>>() {
                            @Override
                            public Tuple3<Long, Long, Float> map(TaxiFare fare) {
                                return Tuple3.of(fare.getEventTimeMillis(), fare.driverId, fare.tip);
                            }
                        }
                )
                .keyBy(t -> t.f1)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .reduce(
                        (t1, t2) -> Tuple3.of(t1.f0, t1.f1, t1.f2 + t2.f2)
                );

        // find the driverId with the highest total tips in each hour
        // and emit a stream of (windowStartTime, driverId, totalTips).
        DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
                .keyBy(t -> t.f0)
                .maxBy(2);


        // print the result on stdout
        hourlyMax.addSink(sink);
        // execute the pipeline and return the result
        return env.execute("Hourly Tips");
    }
}
