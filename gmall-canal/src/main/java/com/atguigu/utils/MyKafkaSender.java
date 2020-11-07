package com.atguigu.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//kafka发送者------也就是kafka生产者，把数据发送到消费Topic
public class MyKafkaSender {
    //属于工具类，所以都是静态方法，类就可以直接调用了
    //KafkaProducer:创建、声明一个生产者对象，用来发送数据
    public static KafkaProducer<String, String> kafkaProducer = null;


    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties(); //为了准备好配置文件
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties); //new了个生产者对象producer
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }

    //写个send方法，里面传个主题和message
    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {//判断kafka生产者是否为空。因为实际生产中，消费者对象可能挂了，那就是个null
            kafkaProducer = createKafkaProducer();//挂了那就重新new个生产者对象。
        }
        //最终调用的还是kafkaProducer.send()。里面要有个生产者对象
        kafkaProducer.send(new ProducerRecord<>(topic, msg));
    }
}

