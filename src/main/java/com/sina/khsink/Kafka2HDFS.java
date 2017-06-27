package com.sina.khsink;

import com.sina.test.Producer;

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
        Thread wTask=new Thread(writeSinkTask);
        Thread rTask=new Thread(readSinkTask);
        wTask.start();
        rTask.start();
    }

    public void init(){
        buffer=new LinkedBlockingQueue();
        writeSinkTask =new WriteSinkTask(buffer,false);
        readSinkTask=new ReadSinkTask(buffer);
    }
}




