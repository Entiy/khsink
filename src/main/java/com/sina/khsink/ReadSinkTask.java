package com.sina.khsink;

import com.sun.org.apache.regexp.internal.RE;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

/**
 * Created by szq on 2017/6/27.
 * 从kafka读数据的Task
 * 功能：
 *      1.将数据写入到队列
 *      2.
 */
public class ReadSinkTask{
    private BlockingQueue buffer=null;
    private KafkaClient kafkaClient=null;
    private ConsumerIterator<String, String> it=null;

    public ReadSinkTask(){
    }
    public ReadSinkTask(BlockingQueue buffer) {
        this();
        this.buffer = buffer;
    }

    public void start(){
        init();
        new Thread(new ReadThread()).start();
    }
    public void init(){
        this.kafkaClient=new KafkaClient();
    }


    class ReadThread implements Runnable{
        public void run() {
            if (buffer!=null){
                it=kafkaClient.consume();
                while (it.hasNext()){
                    try {
                        MessageAndMetadata<String,String> messAndMeta=it.next();
                        String message=messAndMeta.message();
                        String key=messAndMeta.key();
                        String topic=messAndMeta.topic();
                        long offset=messAndMeta.offset();
                        int partition=messAndMeta.partition();
                        String m="CurrentThread:"+Thread.currentThread().getName()+" topic:"+topic+" key:"+key+" message:"+message+" offset:"+offset+" partition:"+partition;
                        buffer.put(m);
                        System.out.println(m);
                        System.out.println("Pulling a message("+message+") from kafka and Putting it into buffer");

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                }
            }
        }

    public BlockingQueue getBuffer() {
        return buffer;
    }

    public void setBuffer(BlockingQueue buffer) {
        this.buffer = buffer;
    }

    public KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    public void setKafkaClient(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
    }

    public ConsumerIterator<String, String> getIt() {
        return it;
    }

    public void setIt(ConsumerIterator<String, String> it) {
        this.it = it;
    }


}