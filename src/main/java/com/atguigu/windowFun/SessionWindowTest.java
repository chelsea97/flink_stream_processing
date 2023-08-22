package com.atguigu.windowFun;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;

public class SessionWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        KeyedStream<Tuple2<String, Integer>, String> keyby = dataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        long timeMills = System.currentTimeMillis();
                        //return Tuple2.of("current time:" + simpleDateFormat.format(timeMills) + value, 1);
                        return Tuple2.of(value,1);
                    }
                })
                .keyBy(tuple -> tuple.f0);
        //Gap表示在会话窗口处理数据期间，如果连续5s没有数据进来，就会触发计算
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyby
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        window
                .sum(1)
                .print();
        ;
        //count number of incident
        keyby.countWindow(3).sum(1).print();
        env.execute("tumblingwindow");
    }
}
