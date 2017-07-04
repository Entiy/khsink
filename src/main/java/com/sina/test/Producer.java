package com.sina.test;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Administrator on 2017/6/27.
 */
public class Producer {

    private boolean flag;

    public static void main(String[] args) {
//        producer();
        new Producer().run();
    }


    public static void producer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "10.13.4.44:9092");
        properties.put("metadata.broker.list", "10.13.4.44:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("request.required.acks", "1");

        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(properties);
        for (int iCount = 0; iCount < 200; iCount++) {
            String message = "My Test Message No " + iCount;
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>("sinktest", message);
            producer.send(record);
        }
        producer.close();
    }

    int count=0;
    public void run(){
        while(!flag){
            System.out.println(flag);
            if (count++>=20)
                flag=true;
        }
    }
}
