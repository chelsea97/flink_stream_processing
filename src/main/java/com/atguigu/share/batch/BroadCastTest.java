package com.atguigu.share.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class BroadCastTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> dataStreamSource = env.readTextFile("data/textfile");
        DataStreamSource<String> ds = env.fromElements("xixi","vivi");
        SingleOutputStreamOperator<String> blacklist = dataStreamSource.map(new RichMapFunction<String, String>() {
            List<String> blacklist = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.blacklist = getRuntimeContext().getBroadcastVariable("blacklist");
            }

            @Override
            public String map(String value) throws Exception {
                for (String s : blacklist) {
                    if (value.contains(s)) {
                        return value;
                    }
                }
                return "!!!" + value;
            }
        });
        /**
         * broadcast variable
         */


        JobExecutionResult result = env.execute();
        Integer acc = result.getAccumulatorResult("acc");
        System.out.println(acc);

    }
}
