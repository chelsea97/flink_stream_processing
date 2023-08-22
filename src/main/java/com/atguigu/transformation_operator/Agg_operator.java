package com.atguigu.transformation_operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Random;

public class Agg_operator {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.fromCollection(Arrays.asList("hello", "hello1", "hello2", "hello3"));
        dataStreamSource
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        String[] s1 = s.split(" ");
                        for (String word:s1){
                            Thread.sleep(1000);
                            collector.collect(Tuple2.of(word,new Random().nextInt(100)));
                        }
                    }
                })
                .map(new MapFunction<Tuple2<String, Integer>, Tuple2<String,Integer>>() {

                    @Override
                    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                        return value;
                    }
                })
                .keyBy(0)
                //.sum(1)
                .max(1)
                .print();
        //流处理任务需要execute触发
        env.execute();
    }
}
