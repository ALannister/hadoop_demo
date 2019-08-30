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
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 ��ȡ������
		FileInputStream fis = new FileInputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg"));

		// 3 ��ȡ�����
		FSDataOutputStream fos = fs.create(new Path("/test/kaola.jpg"));

		// 4 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));

		// 3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala2.jpg"));

		// 4 ���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));

		// 3 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg.part1"));

		// 4 ���ĶԿ�
		byte[] buf = new byte[1024];

		for (int i = 0; i < 300; i++) {
			fis.read(buf);
			fos.write(buf);
		}

		// 5 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}

	@Test
	public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration configuration = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lxf");

		// 2 ��ȡ������
		FSDataInputStream fis = fs.open(new Path("/test/kaola.jpg"));
		
		// 3 ��λ��������λ��
		fis.seek(1024*300);
		
		// 4 ��ȡ�����
		FileOutputStream fos = new FileOutputStream(new File("C:\\Users\\HINOC\\Pictures\\Koala.jpg.part2"));

		// 5���ĶԿ�
		IOUtils.copyBytes(fis, fos, configuration);
		
		// 6 �ر���Դ
		IOUtils.closeStream(fos);
		IOUtils.closeStream(fis);
		fs.close();
	}
}
