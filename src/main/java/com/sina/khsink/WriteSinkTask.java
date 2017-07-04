package com.sina.khsink;

import com.sina.tools.MessageProcess;
import com.sina.tools.MessageProcessImpl;
import com.sina.utils.PropertiesUtils;

import java.util.concurrent.BlockingQueue;

/**
 * Created by szq on 2017/6/27.
 * 将数据写入到HDFS的Task
 * 功能:
 *      1.将队列数据写入到HDFS
 *      2.
 */
public class WriteSinkTask {

    private HDFSClient hdfsClient=null;
    private KafkaClient kafkaClient=null;
    private BlockingQueue buffer=null;
    private MessageProcess messageProcess=null;
    private long flushSize;
    private long total;
    private int count=0;

    public WriteSinkTask(){
    }

    public WriteSinkTask(BlockingQueue buffer,KafkaClient kafkaClient) {
        this.kafkaClient=kafkaClient;
        this.buffer = buffer;
    }

    public void start(){
        init();
        new Thread(new WriteThread()).start();

    }

    public void init(){
        this.hdfsClient=new HDFSClient(kafkaClient);
        this.messageProcess=new MessageProcessImpl();
        this.flushSize= Long.parseLong(PropertiesUtils.getString("flush.size"));
    }

    class WriteThread implements Runnable{
        private boolean isStart=false;
        public void run() {
            if (buffer!=null){
                while(!isStart){
                    try {
                        String message=(String)buffer.take();
                        message= (String) messageProcess.process(message);
                        //hdfsClient.write2HDFS(message.getBytes());
                        System.out.println("Taking a message(" +message.replaceAll("\n","")+ ") from buffer and writing it into HDFS");
                        if (++count>=flushSize){
                            total+=count;
                            hdfsClient.flush();
                            count=0;
//                            isStart=true;
                        }
                        if (total>=200){
                            hdfsClient.flush();
                            hdfsClient.commit();
                            hdfsClient.close();
                            isStart=true;
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    public HDFSClient getHdfsClient() {
        return hdfsClient;
    }

    public void setHdfsClient(HDFSClient hdfsClient) {
        this.hdfsClient = hdfsClient;
    }

    public BlockingQueue getBuffer() {
        return buffer;
    }

    public void setBuffer(BlockingQueue buffer) {
        this.buffer = buffer;
    }

    public MessageProcess getMessageProcess() {
        return messageProcess;
    }

    public void setMessageProcess(MessageProcess messageProcess) {
        this.messageProcess = messageProcess;
    }

    public long getFlushSize() {
        return flushSize;
    }

    public void setFlushSize(long flushSize) {
        this.flushSize = flushSize;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
