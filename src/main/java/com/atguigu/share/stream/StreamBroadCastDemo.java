package com.atguigu.share.stream;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import javax.security.auth.login.Configuration;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamBroadCastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //流处理广播--广播流
        DataStreamSource<String> filterstream = env.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;

            /**
             * 模拟生成source自定义写法
             * @param ctx
             * @throws Exception
             */
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                //测试数据集
                String[] data = new String[]{
                        "java",
                        "scala",
                        "python",
                        "spark",
                        "flink"
                };

                while (isRunning) {
                    TimeUnit.MINUTES.sleep(1);
                    int seed = (int) (Math.random() * data.length);
                    String element = data[seed];
                    ctx.collect(element);
                    System.out.println("当前获取数据：" + element);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        //2.定义广播数据规则
        MapStateDescriptor<String, String> config = new MapStateDescriptor<>("config", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
        //在广播流中添加配置
        BroadcastStream<String> broadcast = filterstream.broadcast(config);

        //3.定义广播过滤数据集
        DataStreamSource<String> dataStreamSource = env.addSource(new RichSourceFunction<String>() {
            private boolean isRunning = true;
            //测试数据集
            String[] data = new String[]{
                    "java",
                    "scala",
                    "python",
                    "spark",
                    "flink"
            };

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                while (isRunning) {
                    TimeUnit.SECONDS.sleep(5);
                    int seed = (int) (Math.random() * data.length);
                    String element = data[seed];
                    ctx.collect(element);
                    System.out.println("当前获取数据：" + element);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        /**
         * 通过指定广播数据进行数据的拦截获取处理，datastream连接方式为connect
         * 将广播流与真实数据连接起来
         */
        SingleOutputStreamOperator<String> result = dataStreamSource.connect(broadcast).process(new BroadcastProcessFunction<String, String, String>() {
            //定义拦截关键字
            public void open(Configuration parameters) throws Exception {
                keywords = "java";
                System.out.println("初始化拦截关键字：" + keywords);
            }

            private String keywords = null;

            /**
             *
             * @param value
             * @param ctx 处理真实数据，因此为read-only
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                if (value.contains(keywords)) {
                    out.collect("拦截消息:" + value + "!" + "," + "该消息中有" + keywords);
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                keywords = value;
                System.out.println("更新拦截关键字：" + value);
            }
        });
        result.print();
    }

}
