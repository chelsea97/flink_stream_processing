package com.atguigu.timeTest;

import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        System.out.println("watermark default interval:"+env.getConfig().getAutoWatermarkInterval());
        env.getConfig().setAutoWatermarkInterval(10000);
        //模拟常规数据，根据系统时间给定时间戳，即有序事件流
        env.addSource(new SourceFunction<Object>() {
            @Override
            public void run(SourceContext<Object> ctx) throws Exception {
                long timeMillis = System.currentTimeMillis();
                String str = timeMillis+"\tnihao\t";
                String[] split = str.split("\t");
                Long timestamp = Long.valueOf(split[0]);
                String data = split[1];
                //发出一条数据以及数据对应的timestamp
                ctx.collectWithTimestamp(data,timestamp);
                //发出一条watermark
                ctx.emitWatermark(new Watermark(timestamp - 1000));
            }

            @Override
            public void cancel() {

            }
        });
        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);
        //通过assignTimestampAndWaterMarks()在这边实现两个方法，指派时间，创建水位线，默认水位线产生周期200ms
        //一般在source后直接assignTimestampandwatermarks调用该方法，或者window operator之后，
        //SingleOutputStreamOperator<String> operator = dataStreamSource.assignTimestampsAndWatermarks((WatermarkStrategy<String>) new MyWaterMark());
        SingleOutputStreamOperator<String> operator = dataStreamSource.assignTimestampsAndWatermarks((WatermarkStrategy<String>) new My_new_WaterMark());
        operator.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                String log = split[0];
                return Tuple2.of(log,1);
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1)
                .printToErr();
        env.execute("watermark");
    }

    private static class MyWaterMark implements WatermarkStrategy<String>, AssignerWithPeriodicWatermarks<String>{
        //定义系统中，数据容忍度，以及每次操作的事件时间。
        long maxLateTime = 5000;
        long currentMaxTimeStamp = 0;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Watermark wm = null;
        @Override
        public WatermarkGenerator<String> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return null;
        }
        //定义事件格式

        //create watermark
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            //水印和容忍度结合
            Watermark wm = new Watermark(currentMaxTimeStamp - maxLateTime);  //maxLateTime,所能容忍时间
            System.out.println("当前水位线"+wm+",当前水位线对应时间戳是："+format.format(wm.getTimestamp()));
            return wm;
        }

        //流中数据
        //element,流中数据
        //recordTimestamp,上一条事件的时间戳
        //return new timestamp
        @Override
        public long extractTimestamp(String element, long recordTimestamp) {
            String[] split = element.split(",");
            String log = split[0];
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Watermark wm = null;
            long timestamp = 0;
            try {
                timestamp = format.parse(split[1]).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            currentMaxTimeStamp = Math.max(timestamp,currentMaxTimeStamp);
            return timestamp;
        }
    }

    private static class My_new_WaterMark implements AssignerWithPunctuatedWatermarks<String> {
        long maxLateTime = 5000;
        long currentMaxTimeStamp = 0;
        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(String lastElement, long extractedTimestamp) {
            String[] split = lastElement.split(",");
            String log = split[0];
            //多一个自定义check业务
            if(log.equals("log")){
                return new Watermark(currentMaxTimeStamp - maxLateTime);
            }
            return null;
        }

        @Override
        public long extractTimestamp(String element, long recordTimestamp) {
            String[] split = element.split(",");
            String log = split[0];
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Watermark wm = null;
            long timestamp = 0;
            try {
                timestamp = format.parse(split[1]).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            currentMaxTimeStamp = Math.max(timestamp,currentMaxTimeStamp);
            return timestamp;
        }
    }
    //private static class
}
