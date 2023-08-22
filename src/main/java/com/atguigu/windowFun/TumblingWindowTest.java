package com.atguigu.windowFun;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Random;

public class TumblingWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        System.out.println(env.getStreamTimeCharacteristic());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
        keyby
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new WindowFunction<Tuple2<String, Integer>, Object, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> input, Collector<Object> collector) throws Exception {
                        int sum = 0;
                        for (Tuple2<String,Integer> tuple2:input){
                            sum += tuple2.f1;
                        }
                        long start = timeWindow.getStart();
                        long end = timeWindow.getEnd();
                        collector.collect("("+sum+")"+"window start"+start+"window end"+end);
                    }

                })
                .print();

        ;
        //count number of incident
        //keyby.countWindow(3).sum(1).print();
        keyby.countWindow(3).apply(new WindowFunction<Tuple2<String, Integer>, Object, String, GlobalWindow>() {
            @Override
            public void apply(String s, GlobalWindow globalWindow, Iterable<Tuple2<String, Integer>> input, Collector<Object> out) throws Exception {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    int sum = 0;
                    for (Tuple2<String,Integer> tuple2:input){
                        sum += tuple2.f1;
                    }
                    //事件全局窗口，基于事件计数
                    long maxTimestamp = globalWindow.maxTimestamp();
                    out.collect("("+sum+")"+"window maxtimestamp"+maxTimestamp);
            }
        }).print();
        env.execute("tumblingwindow");
    }
}

//Processing time, 最小延迟以及最佳性能，但数据处理顺序难以保证，处理事件不能提供确定性，
//对事件到达系统速度和数据流在系统的各个operator之间处理速度很敏感
//Event time,每个事件在其生产设备上产生的事件，对于乱序，延时或者数据重放等情况，都能给出正确的结果
//但同时，事件时间处理通常存在一定的延时，需要为延时和无序事件等待一段时间
//Ingestion time,数据进入flink的时间，source operator中设置的
//Compromise between processing time and event time