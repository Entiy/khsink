package com.sina.khsink;

import com.sina.tools.MessageProcess;
import com.sina.tools.MessageProcessImpl;
import com.sina.utils.PropertiesUtils;
import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

/**
 * Created by qiangshizhi on 2017/7/4.
 */
public class ReadWriteTask {

    private KafkaClient kafkaClient=null;
    private HDFSClient hdfsClient=null;
    private MessageProcess messageProcess=null;
    private long flushSize;
    private long total;
    private int count=0;

    public ReadWriteTask(){
        init();
    }

    public void init(){
        this.kafkaClient=new KafkaClient();
        this.hdfsClient=new HDFSClient(kafkaClient);
        this.messageProcess=new MessageProcessImpl();
        this.flushSize= Long.parseLong(PropertiesUtils.getString("flush.size"));
    }

    public void start(){
        readAndWrite();
    }

    public void readAndWrite(){
        ConsumerIterator<String, String> it=kafkaClient.consume();
        boolean running=true;
        StringBuffer buffer=new StringBuffer();
        while (it.hasNext()&&running){
                try {
                    MessageAndMetadata<String,String> messAndMeta=it.next();
                    String message=messAndMeta.message();
                    String key=messAndMeta.key();
                    String topic=messAndMeta.topic();
                    long offset=messAndMeta.offset();
                    int partition=messAndMeta.partition();
                    String m="CurrentThread:"+Thread.currentThread().getName()+" topic:"+topic+" key:"+key+" message:"+message+" offset:"+offset+" partition:"+partition;
                    System.out.println(m);
                    System.out.println("Pulling a message("+message+") from kafka and Putting it into buffer");
                    message= (String) messageProcess.process(message);
                    buffer.append(message);
                    if (buffer.toString().getBytes().length>=flushSize)
                        hdfsClient.write2HDFS(hdfsClient.getOut(),buffer.toString().getBytes());
//                    System.out.println("Taking a message(" +message.replaceAll("\n","")+ ") from buffer and writing it into HDFS");
//                    if (++count>=flushSize){
//                        total+=count;
//                        hdfsClient.flushAndCommitOffset();
//                        count=0;
//                    }
    //                if (total>=200){
    //                    running=false;
    //                    hdfsClient.flushAndCommitOffset();
    //                    hdfsClient.commit();
    //                    hdfsClient.close();
    //                    kafkaClient.close();
    //
    //                }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

