package com.atguigu.share.batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.List;

public class DistributedCacheTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        environment.registerCachedFile("filepath","cache");
        DataSource<String> source = environment.fromElements("hello", "xixi");
        source.map(new RichMapFunction<String, String>() {
            private StringBuilder dataString = new StringBuilder("");
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("cache");
                List<String> list = FileUtils.readLines(file);
                for (String line : list) {
                    this.dataString.append(line);
                    System.out.println(line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value + dataString;
            }
        });
    }
}
