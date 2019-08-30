package com.lannister.java.demo.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsClient2 {
	@Test
	public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 获取输入流
		FileInputStream fis = new FileInputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg"));

		// 3 获取输出流
		FSDataOutputStream fos = fs.create(new Path("/test/kaola.jpg"));

		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));

		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala2.jpg"));

		// 4 流的对拷
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));

		// 3 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg.part1"));

		// 4 流的对拷
		byte[] buf = new byte[1024];

		for (int i = 0; i < 300; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		// 5 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 获取输入流
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));
		
		// 3 定位输入数据位置
		fis.seek(1024*300);
		
		// 4 获取输出流
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg.part2"));

		// 5流的对拷
		IOUtils.copyBytes(fis, fos, configuration);
		
		// 6 关闭资源
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
}
