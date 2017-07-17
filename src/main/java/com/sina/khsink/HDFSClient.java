package com.sina.khsink;

import com.sina.utils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;


import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

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
    private String ip=null;
    private String category=null;
    private String pid=null;
    private TimeZone timeZone=null;
    private String hdfsURI=null;
    private int currentMinTh=-1;
    private String currentTmpFileName=null;
    private String currentRealFileName=null;
    private static final Logger logger=Logger.getLogger(HDFSClient.class);
    public HDFSClient(KafkaClient kafkaClient){
        this.kafkaClient=kafkaClient;
        init();
    }
    public HDFSClient(){}
    public void init(){
        try {
            topic=PropertiesUtils.getString("sink.topic");
            hdfsURI=PropertiesUtils.getString("hdfs.uri");
            timeZone =TimeZone.getTimeZone("Asia/Shanghai");
            ip=getIpAddress();
            pid=getPid();
            category=PropertiesUtils.getString("category.dir");
            conf=new Configuration();
            conf.set("fs.default.name",hdfsURI);
            fs=FileSystem.get(conf);
            recovery();
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }
    }

    /**
     * 往hdfs中写入数据并检查是否需要提交
     * @param out
     * @param buffer
     */
    public void write2HDFS(FSDataOutputStream out,byte[] buffer){
        try {
            out.write(buffer, 0, buffer.length);
            flushAndCommitOffset();
            logger.debug("Taking "+buffer.length+" bytes from buffer and writing it into HDFS");
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }

    }

    public void renameFile(String sourcePath, String targetPath){
        try {
            final Path srcPath = new Path(sourcePath);
            final Path dstPath = new Path(targetPath);
            if (fs.exists(srcPath)) {
                logger.debug(fs.rename(srcPath,dstPath)?"Commit success and Rename success":"Commit failure and Rename error");
            }
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }

    }

    public String getWriteDir(){
        return hdfsURI+getHadoopLogPath(category,true,timeZone)[0];
    }
    public String getTmpFileNameURI(){

        return getRealFileNameURI()+"-tmp";
    }

    public String getRealFileNameURI(){
        String[] strings=getHadoopLogPath(category,true,timeZone);
        return hdfsURI+strings[0]+strings[1];
    }

    public void flush() {
        try {
            if (out!=null)
                out.hflush();
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }
    }
    public void flushAndCommitOffset(){
        try {

            if (currentMinTh<getMinTh(timeZone)||currentMinTh>getMinTh(timeZone)){
                out.close();
                commit(currentTmpFileName,currentRealFileName);
                kafkaClient.commitOffset();
                currentRealFileName=getRealFileNameURI();
                currentTmpFileName=currentRealFileName+"-tmp";
                out=fs.create(new Path(currentTmpFileName));
                currentMinTh=getMinTh(timeZone);
            } else{
                flush();
                kafkaClient.commitOffset();
            }
        } catch (Exception e) {
            logger.error(e,e.fillInStackTrace());
        }
    }

    public void recovery(){

        FileStatus[] statuses=getFileStatus(getWriteDir());
        long maxTmpFileSize=0;
        Path maxTmpFilePath=null;
        Path preMaxTmpFilePath=null;
        if (statuses!=null&&statuses.length!=0){
            for (int i = 0; i <statuses.length ; i++) {
                FileStatus file=statuses[i];
                if (file.getPath().getName().contains(ip)&&file.getPath().getName().contains("tmp")&&file.getLen()>maxTmpFileSize){
                    maxTmpFileSize=file.getLen();
                    maxTmpFilePath=file.getPath();
                    delete(preMaxTmpFilePath);
                    preMaxTmpFilePath=maxTmpFilePath;
                } else if (file.getPath().getName().contains(ip)&&file.getPath().getName().contains("tmp"))
                    delete(file.getPath());
            }
        }
        try {
            currentRealFileName=getRealFileNameURI();
            currentTmpFileName=currentRealFileName+"-tmp";
            currentMinTh=getMinTh(timeZone);
            out=fs.create(new Path(currentTmpFileName));
            if (maxTmpFilePath!=null&&maxTmpFileSize!=0){
                FSDataInputStream inputStream = fs.open(maxTmpFilePath);
                IOUtils.copyBytes(inputStream,out,40960);
                flush();
                inputStream.close();
                delete(maxTmpFilePath);
            }
        } catch (Exception e) {
            logger.error(e,e.fillInStackTrace());
        }

    }

    public void commit(String tmpFileNameURI,String realFileNameURI){
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
                logger.error(e,e.fillInStackTrace());
            }
    }

    public FileStatus[] getFileStatus(String pathSrc){
        FileStatus[] statuses=null;
        try {
            Path path=new Path(pathSrc);
            if (fs.exists(path))
                statuses=fs.listStatus(path);
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }
        return statuses;
    }

    public void delete(Path path){
        try {
            if (path!=null&&fs.exists(path)){
                fs.delete(path,false);
            }
        } catch (IOException e) {
            logger.error(e,e.fillInStackTrace());
        }
    }


//    public static void main(String[] args) {
//        HDFSClient d=new HDFSClient();
//        TimeZone timeZone =TimeZone.getTimeZone("Asia/Shanghai");
//        d.getHadoopLogPath(category,true,timeZone);
//    }
    /**
     * 拼接路径:</p>
     * path: /user/hdfs/rawlog/${category}/%Y_%m_%d/%H</p>
     * fileName:${category}-%{host}-%{pid}-%Y_%m_%d_%H-%Y%m%d%H%{minf}</p>
     * 数据集-hostname-pid-小时-写入时的第几个五分钟</p>
     * @param category
     * @param useLocalTimeStamp
     * @param timeZone
     * @return
     */
    public  String[] getHadoopLogPath(String category, boolean useLocalTimeStamp, TimeZone timeZone) {
        String []paths = new String[2];
        Calendar calendar = null;

        if(timeZone == null){
            calendar = Calendar.getInstance();
        }else{
            calendar = Calendar.getInstance(timeZone);
        }

        if(!useLocalTimeStamp){
            //calendar.setTimeInMillis(Long.valueOf(headers.get("timestamp")));
        }

        String year = String.valueOf(calendar.get(Calendar.YEAR));
        String month = convertInt2StrFormatter(calendar.get(Calendar.MONTH) + 1);
        String day = convertInt2StrFormatter(calendar.get(Calendar.DAY_OF_MONTH));
        String hour = convertInt2StrFormatter(calendar.get(Calendar.HOUR_OF_DAY));
        int minf = calendar.get(Calendar.MINUTE)/10+calendar.get(Calendar.HOUR_OF_DAY)*6;
        String minfStr = String.valueOf(minf);
        if(minf>=0&&minf<10){
            minfStr = "00"+minfStr;
        }else if(minf>=10&&minf<100){
            minfStr = "0"+minfStr;
        }

        StringBuilder realPath = new StringBuilder();
        StringBuilder realFileName = new StringBuilder();

        realPath.append(category);
        if (!category.endsWith("/") ) {
            realPath.append("/");
        }
        realPath.append(year);
        realPath.append("_").append(month);
        realPath.append("_").append(day);
        realPath.append("/").append(hour).append("/");

        paths[0] = realPath.toString();

        String[] strings=category.split("/");
        String subCategory=strings[strings.length-1];
        realFileName.append(subCategory).append("-")
                .append(ip).append("-")
                .append(pid).append("-")
                .append(year).append("_")
                .append(month).append("_")
                .append(day).append("_")
                .append(hour).append("-")
                .append(year).append(month).append(day).append(hour)
                .append(minfStr);
        paths[1] = realFileName.toString();
        return paths;
    }

    public int getMinTh(TimeZone timeZone){
        Calendar calendar = null;
        if(timeZone == null){
            calendar = Calendar.getInstance();
        }else{
            calendar = Calendar.getInstance(timeZone);
        }
        int minTh= calendar.get(Calendar.MINUTE)/10+calendar.get(Calendar.HOUR_OF_DAY)*6;
        return minTh;
    }

    public String getPid(){
        String name = ManagementFactory.getRuntimeMXBean().getName();
        String pid = name.split("@")[0];
        return pid;
    }

    public String convertInt2StrFormatter(int digit){
        StringBuilder builder = new StringBuilder();
        if(digit<10){
            builder.append("0").append(digit);
        }else{
            builder.append(digit);
        }
        return builder.toString();
    }

    public String getIpAddress(){
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            logger.error(e,e.fillInStackTrace());
        }
        return "";
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

    public KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    public void setKafkaClient(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
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

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(TimeZone timeZone) {
        this.timeZone = timeZone;
    }

    public String getHdfsURI() {
        return hdfsURI;
    }

    public void setHdfsURI(String hdfsURI) {
        this.hdfsURI = hdfsURI;
    }


}
