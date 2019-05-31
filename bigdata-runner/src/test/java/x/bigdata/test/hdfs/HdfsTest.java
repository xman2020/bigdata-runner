/**
 * xxx
 * Copyright (c) 2012-2016 All Rights Reserved.
 */
package x.bigdata.test.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * HDFS测试
 * 
 * @author xman 2018年8月6日
 */
public class HdfsTest extends TestCase {

    @Test
    public void testFileInfo() throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        Path path = new Path("/test");
        FileStatus files[] = hdfs.listStatus(path);

        for (FileStatus file : files) {
            System.out.println(file);
        }

        assertNotNull(files);
    }

    @Test
    public void testMkDir() throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        boolean result = hdfs.mkdirs(new Path("/test2"));

        assertTrue(result);
    }

    @Test
    public void testUpload() throws Exception {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        InputStream input = new FileInputStream(new File("/Users/chenshuyuan/Desktop/user.csv"));
        FSDataOutputStream output = hdfs.create(new Path("/test2/user.csv"));
        IOUtils.copyBytes(input, output, 1024, true);
    }

    @Test
    public void testWriteData() throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FSDataOutputStream output = hdfs.create(new Path("/test2/user.csv"));
        // 不对
        Text.writeString(output, "7;root;null;2018");
    }

    @Test
    public void testDownload() throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://localhost:9000"), conf);
        FSDataInputStream input = hdfs.open(new Path("/test2/user.csv"));
        OutputStream output = new FileOutputStream(new File("/Users/chenshuyuan/Desktop/user2.csv"));
        IOUtils.copyBytes(input, output, 1024, true);
    }
}
