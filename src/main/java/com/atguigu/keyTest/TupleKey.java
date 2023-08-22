package com.atguigu.keyTest;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class TupleKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> textStream = env.socketTextStream("localhost", 9999);
        //对流进行操作
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> map = textStream.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String[] split = value.split(" ");
                return Tuple3.of(split[0], split[1], Integer.parseInt(split[2]));
            }
        });
        //Tuple代表通过选择tuple中元素作为key
        KeyedStream<Tuple3<String,String,Integer>, Tuple> keyedStream = map.keyBy(0);
        KeyedStream<Tuple3<String,String,Integer>, Tuple> keyedStream_1 = map.keyBy(1);
        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream_2 = map.keyBy(0, 1);
        KeyedStream<Tuple3<String, String, Integer>, Tuple> keyedStream_3 = map.keyBy("f1");
        //明确说明选择tuple中第二个元素作为key
        KeyedStream<Tuple3<String, String, Integer>, String> tuple3StringKeyedStream = map.keyBy(tuple -> tuple.f1);
        keyedStream.reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {
                return Tuple3.of(value1.f0+"f0"+value2.f0,value1.f1+"f1"+value2.f1,value1.f2+value2.f2);
            }
        }).print();
        env.execute();
    }
}
