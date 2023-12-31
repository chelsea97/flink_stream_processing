package com.atguigu;

import javafx.scene.control.Tab;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.shaded.curator4.com.google.common.collect.Table;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class TableFunctionExample {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,settings);
        DataStreamSource<String> stream = env.fromElements("hello#world", "bigdata#atguigu");
        tEnv.createTemporaryView("t",stream,$("s"));
        //table api
        tEnv
                .from("t")
                .joinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));
        tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")))
                .select($("s"), $("word"), $("length"));
        //rename fields of the function in Table API
        tEnv
                .from("t")
                .leftOuterJoinLateral(call(SplitFunction.class, $("s")).as("newWord", "newLength"))
                .select($("s"), $("newWord"), $("newLength"));
        //sql写法
        //注册udf函数
        tEnv.createTemporarySystemFunction("SplitFunction",SplitFunction.class);
        Table sqlResult = tEnv.sqlQuery("SELECT s, word, length FROM t, LATERAL TABLE(SplitFunction(s))");

        tEnv.sqlQuery("SELECT s, word, length " +
                "FROM t " +
                "LEFT JOIN LATERAL TABLE(SplitFunction(s)) ON TRUE");
        tEnv.toAppendStream(sqlResult, Row.class).print();
        env.execute();
    }

    //类型注解，flink特有的语法
    //collect为终止操作
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split("#")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }
}
