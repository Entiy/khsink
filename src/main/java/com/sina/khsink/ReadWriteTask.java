package com.sina.khsink;

import com.sina.tools.ByteBuffer;
import com.sina.tools.MessageProcess;
import com.sina.tools.MessageProcessImpl;
import com.sina.utils.PropertiesUtils;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;


/**
 * Created by qiangshizhi on 2017/7/4.
 */
public class ReadWriteTask {

    private KafkaClient kafkaClient=null;
    private HDFSClient hdfsClient=null;
    private MessageProcess messageProcess=null;
    private int flushSize;
    private static final Logger logger=Logger.getLogger(ReadWriteTask.class);
    public ReadWriteTask(){
        init();
    }

    public void init(){
        this.kafkaClient=new KafkaClient();
        this.hdfsClient=new HDFSClient(kafkaClient);
        this.messageProcess=new MessageProcessImpl();
        this.flushSize= Integer.parseInt(PropertiesUtils.getString("flush.size"));
    }

    public void start(){
        readAndWrite();
    }

    public void readAndWrite(){
        ConsumerIterator<String, String> it=kafkaClient.consume();
        boolean running=true;
        ByteBuffer buffer= ByteBuffer.allocate(flushSize);
        while (it.hasNext()&&running){
                try {
                    MessageAndMetadata<String,String> messAndMeta=it.next();
                    String message=messAndMeta.message();
                    String key=messAndMeta.key();
                    String topic=messAndMeta.topic();
                    long offset=messAndMeta.offset();
                    int partition=messAndMeta.partition();
                    message= (String) messageProcess.process(message);
                    try {
                        buffer.put(message.getBytes());
                    }catch (IllegalArgumentException ex){
                        hdfsClient.write2HDFS(hdfsClient.getOut(),buffer.array());
                        buffer.clear();
                        buffer.put(message.getBytes());
                    }
                    logger.debug("BufferSize: "+buffer.size());
                    if (buffer.size()>=flushSize){
                        hdfsClient.write2HDFS(hdfsClient.getOut(),buffer.array());
                        buffer.clear();
                    }

                } catch (Exception e) {
                    logger.error(e,e.fillInStackTrace());
                }
            }
        }
    }

