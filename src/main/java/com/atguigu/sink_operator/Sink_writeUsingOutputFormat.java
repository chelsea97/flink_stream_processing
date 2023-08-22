package com.atguigu.sink_operator;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Sink_writeUsingOutputFormat {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> list = Arrays.asList("hello nihao","hello nihao1","hello nihao2");
        DataStreamSource<String> dataStreamSource = env.fromCollection(list);
        dataStreamSource.writeUsingOutputFormat(new OutputFormat<String>() {
            private String prefix = null;
            @Override
            public void configure(Configuration configuration) {

            }

            //numtasks表示最大并行度,taskNumber是比numTasks
            @Override
            public void open(int tasknumber, int numtasks) throws IOException {
                System.out.println("tasknumber"+tasknumber+",numbers:"+numtasks);
                prefix = tasknumber + 1 + ">";
            }

            @Override
            public void writeRecord(String s) throws IOException {
                System.out.println(prefix+s);
            }

            @Override
            public void close() throws IOException {

            }
        });
    }
}
