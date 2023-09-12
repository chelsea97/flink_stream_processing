package com.atguigu.table;

import com.atguigu.table.bean.Person;
import com.atguigu.table.bean.Shares;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.curator4.com.google.common.collect.Table;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

//stream processing scenario
public class Example1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
//        executionEnvironment.readTextFile("xxx.txt").map(new MapFunction<String, Person>() {
//            @Override
//            public Person map(String value) throws Exception {
//                return new Person("1","2",3,value);
//            }
//        });
        //1. construct data stream source
        //generate stream data
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream("localhost", 9999);
        //2.convert into pojo object stream
        SingleOutputStreamOperator<Shares> streamoperator = dataStreamSource.map(new MapFunction<String, Shares>() {
            @Override
            public Shares map(String value) throws Exception {
                String[] s = value.split(" ");
                return new Shares(s[0], Long.valueOf(s[1]), new Double(s[2]));
            }
        });

        //3. create table execution environment
        StreamingTableEnvironment tableEnvironment = StreamingTableEnvironment.create(executionEnvironment);
        Table table = tableEnvironment.fromDataStream(streamoperator);
        Table result_table = table.select("id","total")
                .where("id = 'eth'");

        tableEnvironment.registerTable("shares",table);
        tableEnvironment.sqlQuery("select * from shares");
        Table result_table_2 = tableEnvironment.sqlQuery("select * from shares");
        //convert tableEnvironment to stream data
        tableEnvironment.toAppendStream(result_table, Row.class).print();
        tableEnvironment.toAppendStream(result_table_2, Row.class).print();

        //if stream data is updated continusly and user wish to do some aggregate operation
        Table resultTable3 = table.groupBy("id").select("id,total.sum as sum1");
        tableEnvironment.toRetractStream(resultTable3,Row.class).print();
        executionEnvironment.execute("table");
}
    }