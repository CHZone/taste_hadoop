package com.chzone.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HDFSApp {
    public static String HDFS_PATH = "hdfs://192.168.1.123:8020";
    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Before
    public void setUp() throws IOException, InterruptedException, URISyntaxException {
        System.out.println("setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "caihuorong");
    }
    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("tearDown");
    }

    /**
     * 列出所有文件
     * 
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     */
    @Test
    public void listFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
        System.out.println("list file method");
        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/"));
        for (FileStatus fStatu : fileStatus) {
            String isDir = fStatu.isDirectory() ? "文件夹" : "文件";
            short replication = fStatu.getReplication();// 副本系数
            long len = fStatu.getLen();
            String path = fStatu.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);

        }
    }

    /**
     * 创建目录
     */
    @Test
    public void createDir() throws IllegalArgumentException, IOException {
        fileSystem.mkdirs(new Path("/javacreatedir"));
    }

    /**
     * 创建文件
     */
    @Test
    public void createFileandWrite() throws Exception {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }

    /**
     * 往文件写数据、失败
     */
    @Test
    public void append() throws IllegalArgumentException, IOException {
        configuration.setBoolean("dfs.support.append", true);
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FSDataOutputStream output = fileSystem.append(new Path("/hdfsapi/test/a.txt"));

//        System.setProperty("hadoop.home.dir", "D:/hadoop");

        output.write("hello hadoop".getBytes());
        output.flush();
        output.close();
    }
    /**
     * 将hdfs上的文件内容读取到本地
     */
    @Test
    public void getContent() throws IllegalArgumentException, IOException{
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }
    /**
     * 将hdfs上的文件重命名
     */
    @Test
    public void rename() throws IOException{
        Path old = new Path("/hdfsapi/test/a.txt");
        Path newpath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(old, newpath);
    }
    /**
     * 提交文件到hdfs
     */
    @Test
    public void put() throws IOException{
        Path local = new Path("src/test/resources/localFile");
        Path hdfsFile = new Path("/test");
        fileSystem.copyFromLocalFile(local, hdfsFile);
    }
    
    @Test
    public void putBigFile() throws IllegalArgumentException, IOException{
        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("D:\\bin\\hadoop-2.6.0-cdh5.7.0.tar.gz")));
        FSDataOutputStream output = fileSystem.create(new Path("/test/hadoop.tar.gz"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print("=");
                    }
                });
        
        IOUtils.copyBytes(in, output, 4096);
        
    }
    @Test
    public void get() throws IOException{
        Path local = new Path("src/test/resources/hfile1");
        Path hdfsFile = new Path("/test/localFile");
//        fileSystem.copyToLocalFile(false,hdfsFile, local,true);
        fileSystem.copyToLocalFile(false,hdfsFile, local,true);
    }
    
    
}
