package com.atguigu;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Calendar;
import java.util.Random;
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
    private boolean running = true;
    public void run(SourceFunction.SourceContext<SensorReading> srcCtx) throws Exception {
        Random rand = new Random();
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for(int i = 0;i < 10;i++){
            sensorIds[i] = "sensor_" + i;
            curFTemp[i] = 65 + (rand.nextGaussian()*20);
        }
        while (running){
            long curlTime = Calendar.getInstance().getTimeInMillis();
            for (int i=0; i<10;i++){
                curFTemp[i] += rand.nextGaussian() * 0.5;
                srcCtx.collect(new SensorReading(sensorIds[i],curlTime,curFTemp[i]));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {

    }
}
