package com.sina.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by Administrator on 2017/6/28.
 */
public class Test {

    public static void main(String[] args) throws IOException {
        testRename();
    }

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
}
