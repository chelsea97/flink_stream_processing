package com.atguigu.cep;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.cep.pattern.Pattern;

public class LoginWarningDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Pattern.begin("begin");
    }
}
