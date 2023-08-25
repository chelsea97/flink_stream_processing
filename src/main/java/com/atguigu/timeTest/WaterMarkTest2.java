package com.atguigu.timeTest;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

/**
 * flink提供水位线产生方法，目前对于延迟数据的处理就是丢弃，现在可以使用侧输出的方式来处理延迟数据，这样不会丢弃数据
 */
public class WaterMarkTest2 {
    //定义侧输出
    private static final OutputTag<String> late = new OutputTag<String>("late", BasicTypeInfo.STRING_TYPE_INFO);
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println("watermark default interval:" + env.getConfig().getAutoWatermarkInterval());
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(10000);
        //模拟常规数据，根据系统时间给定时间戳，即有序事件流
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost",9999);
        /**
         * 用于接口实现方法介绍完毕后，flinki提供写好固定方法
         * AscendingTimestampExtractor,使用场景，有序数据流
         */
//        dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
//            @Override
//            public long extractAscendingTimestamp(String element) {
//                String[] split = element.split(",");
//                Long timestamp = Long.valueOf(split[1]);
//                return timestamp;
//            }
//        }).print();
        /**
         * allow latency,() accepts max outoforderness,
         * 根据延迟度，可以进行愿窗口结果重复计算，超出容忍度的数据，侧输出
         */
//        dataStreamSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(5000)) {
//            @Override
//            public long extractTimestamp(String element) {
//                String[] split = element.split(",");
//                Long timestamp = Long.valueOf(",");
//                return timestamp;
//            }
//        })
//                .keyBy(0)
//                .timeWindow(Time.seconds(10))
//                .allowedLateness(Time.seconds(3000));
//                //.sideOutputLateData()
//                //.print();

        /**
         * method of dealing with late arrive data
         */

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = dataStreamSource.assignTimestampsAndWatermarks(new AscendingTimestampsExtractor<String>() {
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split(",");
                return Long.valueOf(split[1]);
            }
        });
        KeyedStream<String, String> stringStringKeyedStream = stringSingleOutputStreamOperator.keyBy(new KeySelector<String, String>() {

            @Override
            public String getKey(String value) throws Exception {
                return value.split(",")[0];
            }
        });
        SingleOutputStreamOperator<String> process = stringStringKeyedStream
                .timeWindow(Time.seconds(10))
                //设置最大延迟时间
                .allowedLateness(Time.seconds(5))
                //定义侧输出标记，给延迟数据设置标记，根据标记获取延迟数据而不是丢弃
                .sideOutputLateData(late)
                .process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                        System.out.println("当前自任务为:" + getRuntimeContext().getIndexOfThisSubtask());
                        System.out.println("窗口开始时间为：" + context.window().getStart() + "窗口结束时间：" + context.window().getEnd());
                        System.out.println("水位线：" + context.currentWatermark());
                        System.out.println("操作系统系统时间:" + System.currentTimeMillis());
                        Iterator<String> iterator = elements.iterator();
                        for (; iterator.hasNext(); ) {
                            String next = iterator.next();
                            System.out.println("当前窗口数据：" + next);
                            out.collect("数据为：" + next);
                        }
                    }
                });


        //非延迟数据处理流程
        process.print();
        //延迟数据处理
        DataStream<String> output = process.getSideOutput(late);
        output.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "我迟到了"+value;
            }
        }).print();
        env.execute();
    }
}
