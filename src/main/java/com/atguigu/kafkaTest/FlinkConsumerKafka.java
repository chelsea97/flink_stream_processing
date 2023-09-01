package com.atguigu.kafkaTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.util.Properties;

public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("zookeeper.connect","localhost:2181");
        properties.setProperty("group.id","test");
        //DataStream<String> stream = env.addSource(new FlinkConsumerKafka("flink-source",new SimpleStringSchema(),properties));
//        env.addSink(new TwoPhaseCommitSinkFunction<String,ContentTransaction,Object>() {
//            @Override
//            protected void invoke(Object transaction, Object value, Context context) throws Exception {
//
//            }
//
//            @Override
//            protected Object beginTransaction() throws Exception {
//                return null;
//            }
//
//            @Override
//            protected void preCommit(Object transaction) throws Exception {
//
//            }
//
//            @Override
//            protected void commit(Object transaction) {
//
//            }
//
//            @Override
//            protected void abort(Object transaction) {
//
//            }
//        });
        env.execute();
        //DataStream<String> stream = env.addSource(new FlinkKafkaConsumer())
    }
}
