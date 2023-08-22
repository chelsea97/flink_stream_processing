package com.atguigu.source_operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Collector;

public class Source_readTextFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> DataStreamSource = env.readTextFile("data/textfile");
        DataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
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
