package com.atguigu.kafkaTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.Properties;

public class FlinkConsumerKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.setProperty("zookeeper.connect","localhost:2181");
        properties.setProperty("group.id","test");
        //DataStream<String> stream = env.addSource(new FlinkConsumerKafka("flink-source",new SimpleStringSchema(),properties));
        env.execute();
        //DataStream<String> stream = env.addSource(new FlinkKafkaConsumer())
    }
}
