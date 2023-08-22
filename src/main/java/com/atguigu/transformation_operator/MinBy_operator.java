package com.atguigu.transformation_operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MinBy_operator {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setBufferTimeout(1000); //sets the maximum time frequency for flushing of the output buffer
        //trigger flushing only when the output buffer is full
        List<Tuple3<String, String, Integer>> tuple3s = Arrays.asList(Tuple3.of("james", "tall", 800)
                , Tuple3.of("nick", "tall", 1000),
                Tuple3.of("george", "tall", 600),
                Tuple3.of("zach", "short", 300));
        DataStreamSource<Tuple3<String, String, Integer>> dataStreamSource = env.fromCollection(tuple3s);
        dataStreamSource.keyBy(0)
                //.sum(1)
                .min(2)
                .print();
        //流处理任务需要execute触发
        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
