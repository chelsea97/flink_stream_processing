package com.atguigu.source_operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Array;
import java.util.Arrays;

public class Source_fromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataStreamSource = env.fromCollection(Arrays.asList("hello nihao","hello nihao1","hello nihao2"));
        dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, org.apache.flink.util.Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for(String s1:s){
                    out.collect(Tuple2.of(s1,1));
                }
            }
        }).keyBy(0).sum(1).printToErr();
        env.execute();
    }
}
