package com.atguigu;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.lang.reflect.Array;
import java.util.Arrays;

public class StreamingJob_2 {
    public static void main(String[] args) throws Exception {
        //set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //get data source
        DataStreamSource<String> socket = env.socketTextStream("localhost",9999);
//        socket
//                //transformation
//                .flatMap(new FlatMapFunction<String, String>() {
//                    @Override
//                    public void flatMap(String value,Collector<String> out) throws Exception{
//                        String[] s = value.split(" ");
//                        for (String word:s){
//                            out.collect(word);
//                        }
//                    }
//                })
//                .map(new MapFunction<String, Tuple2<String,Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(String value) throws Exception {
//                        return Tuple2.of(value,1);
//                    }
//                })
//                .keyBy(0) //此处单词作为key
//                .sum(1) //对tuple中位置1的值求和
//                //sink
//                .print();


        //lambdaTest(socket);


        //anonymousInnerClass(socket);

        //richFunction(socket);

        //processFun(socket);
        //执行计算
       env.execute("Flink Streaming Java API skeleton");
    }
    //lambda编写方式不识别返回类型，需要手动跟上returns指定类型
    private static void lambdaTest(DataStreamSource<String> socket){
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socket.flatMap((String value, Collector<String> out) -> {
            Arrays.stream(value.split(" ")).forEach(word -> out.collect(word));
        });

        stringSingleOutputStreamOperator.returns(Types.STRING);
        stringSingleOutputStreamOperator
                //tuple是flink的tuple,属性从f0开始定义，可以通过Tuple2.of来替换new
                .map(word->Tuple2.of(word,1)).returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy(0)
                .sum(1)
                .print();
    };
    private static void anonymousInnerClass(DataStreamSource<String> socket){
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = socket.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(s1);
                }
            }
        });
        SingleOutputStreamOperator<Object> map = stringSingleOutputStreamOperator.map(new MapFunction<String, Object>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        KeyedStream<Object, Tuple> objectTupleKeyedStream = map.keyBy(0);
        SingleOutputStreamOperator<Object> sum = objectTupleKeyedStream.sum(1);
        sum.print();

    }
    private static void richFunction(DataStreamSource<String> socket){
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = socket.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            //initial content
            private String jobname = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                jobname = "myfirst flink job";
            }

            @Override
            public void close() throws Exception {
                super.close();
                jobname = null;
                System.out.println("jieshule");
            }

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //similar with sparkContext, define broadcast variable
                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(Tuple2.of(jobname + s1, 1));
                }
            }
        });
        tuple2SingleOutputStreamOperator.keyBy(tuple->tuple.f0).sum(1).print();
    }

    private static void processFun(DataStreamSource<String> socket){
        socket
                .process(new ProcessFunction<String, Tuple2<String,Integer>>() {
            private String jobName = null;
            @Override
            public void open(Configuration parameters) throws Exception{
                jobName = "myfirst flink job";
            }
            @Override
            public void close() throws Exception{
                jobName = null;
                System.out.println("jieshule");
            }
            @Override
            public void processElement(String value,Context ctx,Collector<Tuple2<String,Integer>> out) throws Exception {
                ctx.timerService().registerEventTimeTimer(3000);
                String[] s = value.split(" ");
                for (String s1: s){
                    out.collect(Tuple2.of(jobName+s1,1));
                }

            }
        })
                .setParallelism(2)
                .keyBy(tuple->tuple.f0)
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                        return Tuple2.of(value1.f0, value1.f1+value2.f1);
//                    }
//                });
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
                    //初始化每个key在第一次进来
                    private Integer num = 0;
                    //操作对象为keyedStream,等同于已经分好组，每次处理的数据都是相同key下元素
                    @Override
                    //来一条元素处理一条元素
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String currentKey = ctx.getCurrentKey(); //从环境中获取key
                        num += value.f1;
                        out.collect(Tuple2.of(value.f0, num));
                    }
                })
                .setParallelism(3)
                .print().setParallelism(2);


    }

}


