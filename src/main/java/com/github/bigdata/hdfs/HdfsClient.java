package com.github.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码的常用套路
 * 获取一个客户端对象
 * 执行相关操作命令
 * 关闭资源
 * **/
public class HdfsClient {
    FileSystem fs;

    @Before
    public void into() throws URISyntaxException, IOException, InterruptedException {
        //链接集群NameNode地址
        URI uri = new URI("hdfs://hadoop1:9000");
        //创建一个配置文件
        Configuration configuration = new Configuration();
//        设置集群副本数量为2
//        configuration.set("dfs.replication","2");
        //创建一个用户
        String user = "root";
        fs = FileSystem.get(uri, configuration, user);
    }
    @Test
    //在HDFS上创建一个文件
    public void testMkdir() throws IOException {
        fs.mkdirs(new Path("/"));
    }

    @Test
    //将一个文件拷贝到HDFS
    public void testPut() throws IOException {
        fs.copyFromLocalFile(true, new Path("D://"), new Path("/"));
    }

    @Test
    //将一个文件下载到本地
    public void testGet() throws IOException {
        fs.copyToLocalFile(new Path("/fs.xml"), new Path("D://"));
    }
    @Test
    //删除HDFS文件
    public void testRm() throws IOException {
        fs.delete(new Path("/"));
    }
    @Test
    //文件的移动和改名
    public void testMv() throws IOException {
        //改名
        fs.rename(new Path("/"),new Path("/"));
        //移动
        fs.rename(new Path("/"),new Path("/"));
    }
    @Test
    //查看文件详情
    public void testxq() throws IOException {
        RemoteIterator<LocatedFileStatus> listflie = fs.listFiles(new Path("/"), true);
        while(listflie.hasNext()) {
            LocatedFileStatus fileStatus = listflie.next();

            System.out.println("------------"+fileStatus.getPath()+"-------------");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            //由于返回块信息，只会返回一个地址，所以我们要对他toString()
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    //查看并判断一个目录下的文件是文件夹还是文件
    public void testfile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));

            for(FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    System.out.println("文件："+fileStatus.getPath().getName());
                }else {
                    System.out.println("目录："+fileStatus.getPath().getName());
                }
            }
    }

    @After
    public void close() throws IOException {
        fs.close();
    }
}
