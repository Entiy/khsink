package com.sina.khsink;

import com.sina.tools.MessageProcess;
import com.sina.tools.MessageProcessImpl;

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
    private BlockingQueue buffer=null;
    private MessageProcess messageProcess=null;
    private static long FLUSH_SIZE=50;
    private int count=0;

    public WriteSinkTask(){
    }

    public WriteSinkTask(BlockingQueue buffer) {
        this();
        this.buffer = buffer;
    }

    public void start(){
        init();
        new Thread(new WriteThread()).start();

    }

    public void init(){
        this.hdfsClient=new HDFSClient();
        this.messageProcess=new MessageProcessImpl();
    }

    class WriteThread implements Runnable{
        private boolean isStart=false;
        public void run() {
            if (buffer!=null){
                while(!isStart){
                    try {
                        String message=(String)buffer.take();
                        message= (String) messageProcess.process(message);
                        hdfsClient.write2HDFS(message.getBytes());
                        System.out.println("Taking a message(" +message.replaceAll("\n","")+ ") from buffer and writing it into HDFS");
                        if (++count>=FLUSH_SIZE){
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

}
