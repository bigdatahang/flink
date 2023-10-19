package com.bytedance.flink.learn.transformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class MaxMinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(Tuple2.of("male_1", 4));
        list.add(Tuple2.of("male_2", 3));
        list.add(Tuple2.of("female_1", 4));
        list.add(Tuple2.of("female_2", 5));
        list.add(Tuple2.of("male_3", 9));
        list.add(Tuple2.of("male_4", 9));
        list.add(Tuple2.of("female_3", 4));
        list.add(Tuple2.of("female_4", 10));
        DataStreamSource<Tuple2<String, Integer>> stream = env.fromCollection(list);

        stream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                        String f0 = tuple2.f0;
                        return f0.split("_")[0];
                    }
                })
                .maxBy(1)
                .print();

        env.execute();
    }
}
