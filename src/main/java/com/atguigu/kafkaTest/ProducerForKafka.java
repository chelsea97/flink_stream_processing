package com.atguigu.kafkaTest;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class ProducerForKafka extends Thread {
    static String[] channelNames = new String[]{
            "Spark","Scala","Kafka","Flink","Hadoop","Storm",
            "Hive","Impala","HBase","ML"
    };
    static String[] actionNames = new String[]{"View","Register"};
    private String topic; //发送给kafka数据,topic
    //private KafkaProducer<Integer,String> producerForKafka;
    private static String dateToday;
    private static Random random;
    public ProducerForKafka(String topic){
        dateToday = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.topic = topic;
        random = new Random();
        Properties conf = new Properties();
        //kafka版本升级后，配置参数均发生变化，api也变化
        conf.put("bootstrap.servers","node001:9091,node002:9092,node003:9093");
        conf.put("key.serializer","org.apache.kafka.common.serializaton.StringSerializer");
        conf.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        //producerForKafka = new KafkaProducer<>(conf);
    }
    @Override
    public void run(){
        int counter = 0;
        while (true){
            counter++;
            String message = "joy"+counter;
            //producerForKafka.send(new ProducerRecord<>(topic,message));
            System.out.println(message);
            //每五条数据暂停1s
            if (0 == counter % 5){
                try{
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        new ProducerForKafka("flink-source").start();
    }
}
