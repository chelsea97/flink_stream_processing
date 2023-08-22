package com.atguigu.windowFun;

import org.apache.commons.compress.harmony.archive.internal.nls.Messages;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;

public class SlidingWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        KeyedStream<Tuple2<String, Integer>, String> keyby = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        long timeMills = System.currentTimeMillis();
                        System.out.println("当前时间为:"+simpleDateFormat.format(timeMills)+",具体值为"+value);
                        //return Tuple2.of("current time:" + simpleDateFormat.format(timeMills) + value, 1);
                        return Tuple2.of(value,1);
                    }
                })
                .keyBy(tuple -> tuple.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyby.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> countwindow = keyby.countWindow(5, 2);
        countwindow.sum(1).print();
        env.execute("tumblingwindow");
    }
}
