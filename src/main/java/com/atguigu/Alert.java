package com.atguigu;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class Alert {
    public String message;
    public long timestamp;
    public Alert(String message, long timestamp){
        this.message = message;
        this.timestamp = timestamp;
    }
    public enum SmokeLevel{
        LOW,
        HIGH
    }
    public String toString() {
        return "(" + message + "," + timestamp + ")";
    }
    public static class SmokeLevelSource implements SourceFunction<SmokeLevel> {
        private boolean running = true;
        @Override
        public void run(SourceContext<SmokeLevel> sourceContext) throws Exception {
            Random rand = new Random();
            while (running) {
                if (rand.nextGaussian() > 0.8){
                    srcCtx.collect(SmokeLevel.HIGH);
                } else {
                    srcCtx.collect(SmokeLevel.LOW);
                }
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
