package com.atguigu.operatorChain;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.security.auth.login.AppConfigurationEntry;

/**
 * operation chaining advantage: decrease serialization and deserialization
 * 2. decrease data exchange in data cache area
 * 3. reduce latency and increase throughput
 * 4. reduce thread exchange
 * operation chaining disadvantage: all tasks run in the same slot, make computation slow if task is complex
 * solution: operator new chain and disable chain manually
 * taskslots assigns taskmanager's memory
 */


public class OperatorChainTest {
    public static void main(String[] args) throws Exception{

        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setString("taskmanager.numberofSlots","3");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //disable operator chaining for streaming operator
        env.disableOperatorChaining();
        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements("java", "scala", "spark");
        DataStreamSource<String> source_1 = env.socketTextStream("localhost", 9999);
        source_1.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("!!!!!"+value);
                return value;
            }
        }).disableChaining().map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("22222"+value);
                return value;
            }
        }).startNewChain()
                //group slot
                //if two configuration are not in the same group, these two operators cannot share the same group
                //different operators will occupy other slot
                .slotSharingGroup("sharegroup");
        env.execute();
    }
}
