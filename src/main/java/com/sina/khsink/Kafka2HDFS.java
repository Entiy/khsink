package com.sina.khsink;

import com.sina.test.Producer;
import com.sina.utils.PropertiesUtils;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Hello world!
 *
 */
public class Kafka2HDFS {

    private WriteSinkTask writeSinkTask=null;
    private ReadSinkTask readSinkTask=null;
    private BlockingQueue buffer=null;
    public static void main(String[] args) {
        Producer.producer();//生产数据
        new Kafka2HDFS().start();
    }
    public void start() {
        init();
//        for (int i = 0; i < 3; i++) {
            readSinkTask=new ReadSinkTask(buffer);
            new Thread(readSinkTask,"ThreadA").start();
//        }
        new Thread(writeSinkTask).start();

    }

    public void init(){
        PropertiesUtils.load("src/main/java/com/sina/conf/khsink.properties");
        buffer=new LinkedBlockingQueue();
        writeSinkTask =new WriteSinkTask(buffer,false);
//        readSinkTask=new ReadSinkTask(buffer);
    }
}




