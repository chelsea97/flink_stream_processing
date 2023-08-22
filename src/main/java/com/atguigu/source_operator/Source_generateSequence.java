package com.atguigu.source_operator;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source_generateSequence {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //process data line by line
        DataStream<Long> dataStreamSource = env.generateSequence(0,100);
        SingleOutputStreamOperator<Long> filter = dataStreamSource.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 5 == 0;
            }
        });
        filter.print();
        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
