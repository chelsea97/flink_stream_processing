package com.atguigu.share.stream;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class AccTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> dataStreamSource = env.readTextFile("data/textfile");
        DataStreamSource<String> ds = env.fromElements("xixi","vivi");
        /**
         * accumulator
         */
         dataStreamSource.map(new RichMapFunction<String,String>() {
             private IntCounter intCounter = new IntCounter();
             List<String> black = null;
             @Override
             public void open(Configuration parameters) throws Exception{
//                 List<String> broadcast = getRuntimeContext().getBroadcastVariable("broadcast");
//                 super.open(parameters);
                 getRuntimeContext().addAccumulator("acc",this.intCounter);
             }
             @Override
             public String map(String value) throws Exception {
                 this.intCounter.add(1);
                 return value;
             }
         }).print();

        JobExecutionResult result = env.execute();
        Integer acc = result.getAccumulatorResult("acc");
        System.out.println(acc);

    }
}
