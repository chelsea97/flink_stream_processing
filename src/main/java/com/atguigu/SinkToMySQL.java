package com.atguigu;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import javax.security.auth.login.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); //setup stream time characteristic
        env.getConfig().setAutoWatermarkInterval(5000); //setup time interval of watermark, generate watermark every 5 seconds
        DataStream<SensorReading> readings = env
                .addSource(new SensorSource())
                .filter(r -> r.temperature > 25)
                .assignTimestampsAndWatermarks(new MyAssigner());
        DataStream<SensorReading> stream = env.addSource(new SensorSource());
        stream.addSink(new MyJDBCSink());
        env.execute();
    }
    public static class MyJDBCSink extends RichSinkFunction<SensorReading> {
        private Connection conn;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            conn = DriverManager.getConnection("\"jdbc:mysql://localhost:3306/sensor\",\n" +
                    "                    \"zuoyuan\",\n" +
                    "                    \"zuoyuan\"");
            insertStmt = conn.prepareStatement("INSERT INTO temps (id, temp) VALUES (?, ?)"); //sending parameterized SQL to database
            updateStmt = conn.prepareStatement("UPDATE temps SET temp = ? WHERE id = ?"); //sending parameterized SQL to database
        }

        public void invoke(SensorReading value, Context context) throws Exception {
            updateStmt.setDouble(1, value.temperature);
            //Sets the designated parameter to the given Java double value.
            // The driver converts this to an SQL DOUBLE value when it sends it to the database.
            updateStmt.setString(2, value.id);

            if(updateStmt.getUpdateCount() == 0){
                insertStmt.setString(1, value.id);
                insertStmt.setDouble(2,value.temperature);
                insertStmt.execute();
            }
        }

        public void close() throws Exception {
            super.close();
            insertStmt.close();
            updateStmt.close();
        }
    }

}
