package com.atguigu;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import javax.security.auth.login.Configuration;

public class SensorSwitch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<SensorReading,String> stream = env.addSource(new SensorSource()).keyBy(r -> r.id);
        KeyedStream<Tuple2<String,Long>,String> switches = env.fromElements(Tuple2.of("sensor_2",10*100L));
        stream.connect(switches).process(new SwitchProcess()).print();
        env.execute();
    }
    public static class SwitchProcess extends CoProcessFunction<SensorReading,Tuple2<String,Long>,SensorReading>{
        private ValueState<Boolean> forwardingEnabled;
        public void open(Configuration parameters) throws Exception{
            super.open(parameters);
            forwardingEnabled = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("filterSwitch", Types.BOOLEAN)
            );
            //getRuntimeContext() gets the context that contains information about the UDF's runtime
            //such as the parallelism of the function, the subtask index of the function
            //On each access, the state exposes the value for the key of the element currently processed by the function.
            //get a handle to the system's key/value state.
        }

        @Override
        public void processElement1(SensorReading sensorReading, CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
            if (forwardingEnabled.value() != null && forwardingEnabled.value()){
                collector.collect(sensorReading);
            }
        }

        @Override
        public void processElement2(Tuple2<String, Long> stringLongTuple2, CoProcessFunction<SensorReading, Tuple2<String, Long>, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
            forwardingEnabled.update(true);
            context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime()+stringLongTuple2.f1); //register time mechanism
        }
        public void onTimer(long timestamp,OnTimerContext ctx,Collector<SensorReading> collector) throws Exception{
            super.onTimer(timestamp, ctx, collector);
            forwardingEnabled.clear();
        }
    }
}
