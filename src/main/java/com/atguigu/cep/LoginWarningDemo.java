package com.atguigu.cep;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LoginWarningDemo {
    public static <SimpleDataFormat> void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Pattern<Object, Object> pattern = Pattern.begin("begin");
        //define complex and combination filter using and / or.
        Pattern<Object, Object> pattern1 = pattern.where(new IterativeCondition<Object>() {
            @Override
            public boolean filter(Object value, Context<Object> ctx) throws Exception {
                return value.equals(1);
            }
        }).or(new IterativeCondition<Object>() {
            @Override
            public boolean filter(Object value, Context<Object> ctx) throws Exception {
                return value.equals(2);
            }
        }).times(2, 4).optional().within(Time.milliseconds(10));//add optional to agree that pattern is not necessary satisfied to filter function

        //use next or followBy to set up multiple incident connections
        Pattern<Object, Object> pattern2 = pattern1.next("next");
        Pattern<Object, Object> pattern3 = pattern1.followedBy("follow");

        Pattern<LoginEvent, LoginEvent> pattern_new = Pattern.<LoginEvent>begin("first").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.getType().equals("fail");
            }
        }).next("second").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.getType().equals("fail");
            }
        }).next("third").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value, Context<LoginEvent> ctx) throws Exception {
                return value.getType().equals("fail");
            }
        }).within(Time.minutes(1));

        DataStreamSource<String> textfile = env.readTextFile("///");
        SingleOutputStreamOperator<LoginEvent> map = textfile.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(split[0],split[1],split[2],split[3]);
            }
        });

        SingleOutputStreamOperator<LoginEvent> loginEventSingleOutputStreamOperator = map.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            SimpleDateFormat simpleDataFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            public long extractAscendingTimestamp(LoginEvent element) {
                Date date = null;
                try {
                    date = simpleDataFormat.parse(element.getTimestamp());
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                return date.getTime();
            }
        });
        //generate pattern stream
        PatternStream<LoginEvent> patternstream = CEP.pattern(loginEventSingleOutputStreamOperator.keyBy("Id"), pattern_new);

        //select data
        SingleOutputStreamOperator<String> select = patternstream.select(new PatternSelectFunction<LoginEvent, String>() {

            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> third_events = pattern.get("third");
                //LoginEvent loginEvent = third_events.get(0);
                String result_string = null;
                for (LoginEvent third_event : third_events) {
                    result_string = third_event.toString();
                    System.out.println("result string is :"+result_string);
                }
                return result_string;
            }
        });
        select.printToErr();
        env.execute("cep");

    }
}
