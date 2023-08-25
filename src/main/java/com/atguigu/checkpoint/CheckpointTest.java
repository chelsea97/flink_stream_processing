package com.atguigu.checkpoint;

import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.management.ObjectName;
import java.lang.management.MemoryManagerMXBean;

/**
 * checkpoint与savepoint区别，checkpoint目的是在job意外失败时提供恢复机制
 * checkpoint生命周期由flink管理，即flink创建，拥有和发布checkpoint
 * savepoint由用户创建，拥有和删除，有计划的手动备份和恢复
 */
public class CheckpointTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //setup interval between last checkpoint and current checkpoint
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        //2.get checkpoint setting and set up
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //set up system consistency in the case of failure
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //sets the minimal pause between checkpointing attempts.
        //This setting defines how soon the checkpoint corrdinator may trigger another checkpoint
        //after it becomes possible to trigger another checkpoint with respect to max number of concurent
        //set up minimal interval time between checkpoint
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        //how many checkpoint can run simultaneously
        checkpointConfig.setMaxConcurrentCheckpoints(5);
        //sets the maximal time that a checkpoint may take before being discarded
        checkpointConfig.setCheckpointTimeout(50000);

        //checkpoint start data persistence
        //delete_on_cancellation, delete canceled task's status and data automatically
        //retain_on_cancellation, save canceled task's status and data automatically
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //use memory method to save snapshot value
        env.setStateBackend(new MemoryStateBackend(10 * 1024 * 1024, true));
        DataStreamSource<String> source = env.socketTextStream("localhost",9999);
        try{
            env.execute();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
