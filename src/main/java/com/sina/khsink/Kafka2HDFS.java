package com.sina.khsink;

import com.sina.test.Producer;
import com.sina.utils.PropertiesUtils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 *
 */
public class Kafka2HDFS {

//    private WriteSinkTask writeSinkTask=null;
//    private ReadSinkTask readSinkTask=null;
//    private BlockingQueue buffer=null;
//    public static void main(String[] args) {
//        Producer.producer();//生产数据
//        new Kafka2HDFS().start();
//    }
//    public void start() {
//        init();
//        readSinkTask=new ReadSinkTask(buffer);
//        writeSinkTask =new WriteSinkTask(buffer,readSinkTask.getKafkaClient());
//        readSinkTask.start();
//        writeSinkTask.start();
//    }
//
//    public void init(){
//        PropertiesUtils.load("src/main/java/com/sina/conf/khsink.properties");
//        buffer=new LinkedBlockingQueue();
////        writeSinkTask =new WriteSinkTask(buffer);
////        readSinkTask=new ReadSinkTask(buffer);
//    、

    private ReadWriteTask readWriteTask=null;
    public static void main(String[] args) {
        Producer.producer();
        new Kafka2HDFS().start();
    }

    public void start(){
        init();
        readWriteTask.start();
    }
    public void init(){
        PropertiesUtils.load("src/main/java/com/sina/conf/khsink.properties");
        readWriteTask=new ReadWriteTask();
    }
}




