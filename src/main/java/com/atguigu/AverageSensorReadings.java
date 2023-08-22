package com.atguigu;
import akka.japi.tuple.Tuple3;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import javax.xml.crypto.Data;
import java.util.Properties;

public class AverageSensorReadings {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();, create local environment
        //StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment("host",1234,"path"), create local environment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //从数据流将数据摄取到程序中，创建类型为sensorReading的数据流
        DataStream<SensorReading> sensorData = env.addSource(new SensorSource());

        DataStream<T> avgTemp = sensorData.map(r-> {
            Double celsius = (r.temperature - 32) * (5.0 / 9.0);
            return new SensorReading(r.id,r.timestamp,celsius);
        }).keyBy(r->r.id).timeWindow(Time.seconds(5)).apply(new TemperatureAverager());
        avgTemp.print();
        env.execute("Compute average sensor temperature");
        DataStream<SensorReading> stream = env.fromElements(
                new SensorReading("sensor_1",1547718199, 35.80018327300259),
                new SensorReading("sensor_6", 1547718199, 15.402984393403084)
        );
        //read stream data from file
        DataStream<SensorReading> stream2 = env.readTextFile(filepath);
        //read from kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id","consumer-group");
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env1.setParallelism(1); //set parallelism degree

        DataStream<String> sensorIds = stream2.map(new IdExtractor());
        DataStream<String> sensorIds_1 = stream2.map(r -> r.id); //use lambda function to simplify map process
        DataStream<SensorReading> sensorIds_2 = stream2.filter(r -> r.temperature >= 25); //filter operation

        class IdExtractor implements MapFunction<SensorReading,String>{
            @Override
            public String map(SensorReading sensorReading) throws Exception {
                return sensorReading.id;
            }
        }
        class IdSplitter implements FlatMapFunction<String,String> {
            @Override
            public void flatMap(String id, Collector<String>out) {
                String[] splits = id.split("_");
                for (String split:splits){
                    out.collect(split);
                }
            }
        }

        DataStream<String> splitIds = sensorIds
                .flatMap((FlatMapFunction<String, String>)
                        (id, out) -> { for (String s: id.split("_")) { out.collect(s);}})
                // provide result type because Java cannot infer return type of lambda function
                // 提供结果的类型，因为Java无法推断匿名函数的返回值类型
                .returns(Types.STRING);

        DataStream<SensorReading> maxTemPerSensor = keyed.reduce((r1,r2)-> {
            if (r1.temperature > r2.temperature) {
                return r1;
            }else {
                return r2;
            }
        });

        KeyedStream<SensorReading,String> keyedStream = stream.keyBy(r -> r.id);

        DataStream<Tuple3<Integer, Integer, Integer>> inputStream = env.fromElements(new Tuple3(1, 2, 2), new Tuple3(2, 3, 1), new Tuple3(2, 2, 4), new Tuple3(1, 5, 3));
        DataStream<Tuple3<Integer,Integer,Integer>> resultStream = inputStream.keyBy(0).sum(1); //key on first field of the tuple, sum the second field of the tuple in place
        DataStream<SensorReading> twostream = stream.union(stream2);
        DataStream<SensorReading> twostream_1 = stream.union(stream2);

        DataStream<Tuple2<Integer,Long>> one = ...;


    }
}
