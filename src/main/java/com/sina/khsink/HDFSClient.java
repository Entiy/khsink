package com.sina.khsink;

import com.sina.utils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.util.UUID;

/**
 * Created by szq on 2017/6/26.
 * HDFS客户端工具
 * 功能：
 *     1.将队列数据写入到HDFS DataQueue
 *     2.
 */
public class HDFSClient {
    private Configuration conf=null;
    private FileSystem fs=null;
    private KafkaClient kafkaClient=null;
    private FSDataOutputStream out=null;
    private String realFileNameURI=null;
    private String topic=null;
    private String tmpFileNameURI=null;
    private String writeDir=null;
    private String ip=null;
    public HDFSClient(KafkaClient kafkaClient){
        this.kafkaClient=kafkaClient;
        init();
    }
    public void init(){
        try {
            conf=new Configuration();
            conf.set("fs.default.name",PropertiesUtils.getString("hdfs.uri"));
            writeDir=PropertiesUtils.getString("hdfs.uri")+PropertiesUtils.getString("write.dir");
            topic=PropertiesUtils.getString("sink.topic");
            tmpFileNameURI=getTmpFileNameURI();
            realFileNameURI=getRealFileNameURI();
            fs=FileSystem.get(conf);
            recovery();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void write2HDFS(FSDataOutputStream out,byte[] buf){
        try {
            out.write(buf, 0, buf.length);
            flushAndCommitOffset();
            System.out.println("Taking "+buf.length+" bytes from buffer and writing it into HDFS");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void renameFile(String sourcePath, String targetPath){
        try {
            final Path srcPath = new Path(sourcePath);
            final Path dstPath = new Path(targetPath);
            if (fs.exists(srcPath)) {
                System.out.println(fs.rename(srcPath,dstPath)?"Rename success":"Rename error");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public String getTmpFileNameURI(){
        try {
            ip = InetAddress.getLocalHost().getHostAddress().replaceAll("\\.","");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return writeDir+topic+"_"+ UUID.randomUUID().toString()+"_"+ip+"_tmp";
    }

    public String getRealFileNameURI(){
        return "hdfs://10.210.136.61:8020/szq/sinkTest";
    }

    public void flush() {
        try {
            if (out!=null)
                out.hflush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void flushAndCommitOffset(){
        try {
            if (out!=null){
                out.hflush();
                kafkaClient.commitOffset();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void recovery(){

        long maxTmpFileSize=0;
        Path maxTmpFilePath=null;
        FileStatus[] statuses=getFileStatus(writeDir);
        for (FileStatus file:statuses) {
            if (file.getPath().getName().contains(ip)&&file.getLen()>maxTmpFileSize){
                maxTmpFileSize=file.getLen();
                maxTmpFilePath=file.getPath();
            }
        }
        for (FileStatus file:statuses) {
            if (!file.getPath().getName().equals(maxTmpFilePath.getName())){
                delete(file.getPath());
            }
        }
        try {
            out=fs.create(new Path(tmpFileNameURI));
            if (maxTmpFilePath!=null&&maxTmpFileSize!=0){
                FSDataInputStream inputStream = fs.open(maxTmpFilePath);
                IOUtils.copyBytes(inputStream,out,40960);
                flush();
                inputStream.close();
                delete(maxTmpFilePath);
            }
        } catch (Exception e) {
            System.exit(0);
            e.printStackTrace();
        }

    }

    public void commit(){
        renameFile(tmpFileNameURI,realFileNameURI);
    }

    public void close(){
        if (out!=null)
            try {
                out.close();
                out=null;
                if (fs!=null){
                    fs.close();
                    fs=null;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
    }

    public FileStatus[] getFileStatus(String pathSrc){
        FileStatus[] statuses=null;
        try {
            Path path=new Path(pathSrc);
            statuses=fs.listStatus(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return statuses;
    }

    public void delete(Path path){
        try {
            if (fs.exists(path)){
                fs.delete(path,false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public FSDataOutputStream getOut() {
        return out;
    }

    public void setOut(FSDataOutputStream out) {
        this.out = out;
    }

    public void setRealFileNameURI(String realFileNameURI) {
        this.realFileNameURI = realFileNameURI;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setTmpFileNameURI(String tmpFileNameURI) {
        this.tmpFileNameURI = tmpFileNameURI;
    }

    public String getWriteDir() {
        return writeDir;
    }

    public void setWriteDir(String writeDir) {
        this.writeDir = writeDir;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
