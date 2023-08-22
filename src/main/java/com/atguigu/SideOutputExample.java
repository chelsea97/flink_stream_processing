package com.atguigu;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputExample {
    private static OutputTag<String> output = new OutputTag<String>("side-output"){};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        SingleOutputStreamOperator<SensorReading> warnings = stream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.temperature < 32){
                    context.output(output,"温度小于32度");
                }
                collector.collect(sensorReading);
            }
        });
        warnings.print();
        warnings.getSideOutput(output).print();
        env.execute();
    }
}
