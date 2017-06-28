package com.sina.khsink;

import com.sina.utils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
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
    private FSDataOutputStream out=null;
    private String uri=null;
    private String topic=null;
    public HDFSClient(){
        init();
    }
    public void init(){
        try {
            conf=new Configuration();
            uri=tmpFileName();
            fs=FileSystem.get(URI.create(uri),conf);
            Path path=new Path(uri);
            out=fs.create(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void write2HDFS(byte bytes[]){
        try {
            out.write(bytes,0,bytes.length);
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
    public String tmpFileName(){
        topic=PropertiesUtils.getString("sink.topic");
        return PropertiesUtils.getString("write.dir")+topic+"_"+ UUID.randomUUID()+"_tmp";
    }

    public void recovery(){
        //todo
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

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }
}
