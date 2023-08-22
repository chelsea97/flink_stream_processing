package com.atguigu.keyTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class ClassKeyTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> textstream = env.socketTextStream("localhost",9999);
        KeyedStream<String, String> stringStringKeyedStream = textstream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.split(" ")[5];
            }
        });
        SingleOutputStreamOperator<StudentInfo> map = textstream.map(new MapFunction<String, StudentInfo>() {
            @Override
            public StudentInfo map(String value) throws Exception {
                String[] split = value.split(" ");
                String name = split[0];
                String gender = split[1];
                Integer age = Integer.parseInt(split[2]);
                String className = split[3];
                Integer score1 = Integer.parseInt(split[4]);
                Integer score2 = Integer.parseInt(split[5]);
                return new StudentInfo(name, gender, age, Tuple2.of(className, Tuple2.of(score1, score2)));
            }
        });
        KeyedStream<StudentInfo, Tuple> name = map.keyBy("name");
        KeyedStream<StudentInfo, Tuple> studentInfoTupleKeyedStream = map.keyBy("classAndScore.f0");
        KeyedStream<StudentInfo, String> studentInfoStringKeyedStream = map.keyBy(student -> student.getName());
        studentInfoStringKeyedStream.reduce(new ReduceFunction<StudentInfo>() {
            @Override
            public StudentInfo reduce(StudentInfo value1, StudentInfo value2) throws Exception {
                String name = value1.getName()+":"+value2.getName();
                String gender = value1.getGender() + ":"+value2.getGender();
                Integer age = value1.getAge()+value2.getAge();
                Tuple2<String,Tuple2<Integer,Integer>> cas1 = value1.getClassAndScore();
                Tuple2<String,Tuple2<Integer,Integer>> cas2 = value2.getClassAndScore();
                Tuple2<String, Tuple2<Integer, Integer>> cas3 = Tuple2.of(cas1.f0 + ":" + cas2.f0, Tuple2.of(cas1.f1.f0 + cas2.f1.f0, cas1.f1.f1 + cas2.f1.f1));
                return new StudentInfo(name,gender,age,cas3);
            }
        }).print();
    }
}
