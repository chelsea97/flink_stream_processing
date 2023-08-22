package com.atguigu;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

public class RedirectLateEvent {
    private static OutputTag<String> output = new OutputTag<String>("late-readings"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<Tuple2<String,Long>> stream = env.socketTextStream("localhost",9999).map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split("");
                return Tuple2.of(arr[0],Long.parseLong(arr[1])*1000L);
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String,Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
        ).process(new ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>() {
            @Override
            public void processElement(Tuple2<String, Long> stringLongTuple2, ProcessFunction<Tuple2<String, Long>, Tuple2<String, Long>>.Context context, Collector<Tuple2<String, Long>> collector) throws Exception {
                if (stringLongTuple2.f1 < context.timerService().currentWatermark()){
                    context.output(output,"late event is comming!");
                }else {
                    collector.collect(stringLongTuple2);
                }
            }
        });
        stream.print();
        stream.getSideOutput(output).print();
        env.execute();
    }
}
