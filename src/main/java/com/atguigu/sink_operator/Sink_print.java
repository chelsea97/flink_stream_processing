package com.atguigu.sink_operator;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

public class Sink_print {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        List<String> list = Arrays.asList("hello nihao","hello nihao1","hello nihao2");
        DataStreamSource<String> dataStreamSource = env.fromCollection(list);
        DataStreamSink<String> sink = dataStreamSource.writeAsText("./data/write/Text.txt", FileSystem.WriteMode.OVERWRITE);
        //dataStreamSource.writeAsCsv("./data/write/Text.csv", FileSystem.WriteMode.OVERWRITE);
        //把对象转化为可传输的字节序列结构
        dataStreamSource.writeToSocket("localhost", 9999, new SerializationSchema<String>() {
            @Override
            public byte[] serialize(String s) {
                return new byte[0];
            }
        });
        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
