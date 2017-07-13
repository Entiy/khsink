package com.sina.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;

/**
 * Created by Administrator on 2017/6/28.
 */
public class Test {

    public static void testRename() throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.default.name","hdfs://10.210.136.61:8020");
        FileSystem fs=FileSystem.get(configuration);
        Path from=new Path("/szq/sinktest-3733acfb-f08c-4a30-ac12-f6244d76517b");
        Path to=new Path("/szq/sinktest");
        boolean isRename=fs.rename(from,to);
        String result=isRename?"success":"error";
        System.out.println(result);
    }

    public static void main(String[] args) throws IOException {

//        testRename();
//        String ip = InetAddress.getLocalHost().getHostAddress();
//        ip=ip.trim().replaceAll("\\.","");
//        System.out.println(ip);
       //list();
//        getPid();
        existDir();
    }

    public  static  void list() throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.default.name","hdfs://10.210.136.61:8020");
        FileSystem fs=FileSystem.get(configuration);
        FileStatus[] statuses=fs.listStatus(new Path("hdfs://10.210.136.61:8020/szq"));
        for (FileStatus file:statuses) {

            System.out.println(file.getPath().getName());

        }
    }

    public static void existDir() throws IOException {
        Configuration configuration=new Configuration();
        configuration.set("fs.default.name","hdfs://10.210.136.61:8020");
        FileSystem fs=FileSystem.get(configuration);
        FsPermission filePermission = null;
        filePermission = new FsPermission(
                FsAction.ALL, //user action
                FsAction.ALL, //group action
                FsAction.READ);//other action
            FSDataOutputStream input=fs.create(new Path("hdfs://10.210.136.61:8020/szq1/sdad/dasda.txt"));
            input.write("hello".getBytes());
            input.close();

    }
    public static String getPid(){
        String name = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println(name);
        String pid = name.split("@")[0];
        System.out.println("Pid"+pid);
        return pid;
    }
}
