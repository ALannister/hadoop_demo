package com.lannister.maven.demo.hadoop;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {
	@Test
	public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 ����Ŀ¼
		fs.mkdirs(new Path("/test/�иּ���"));

		// 3 �ر���Դ
		fs.close();
	}

	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 �ϴ��ļ�
		fs.copyFromLocalFile(new Path("C:\\Users\\HINOC\\Pictures\\�ޱ���.png"), new Path("/test/����.png"));

		// 3 �ر���Դ
		fs.close();

		System.out.println("over");
	}

	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 ִ�����ز���
		// boolean delSrc ָ�Ƿ�ԭ�ļ�ɾ��
		// Path src ָҪ���ص��ļ�·��
		// Path dst ָ���ļ����ص���·��
		// boolean useRawLocalFileSystem �Ƿ����ļ�У��
		fs.copyToLocalFile(false, new Path("/test/����.png"), new Path("C:\\Users\\HINOC\\Pictures\\����.png"), true);

		// 3 �ر���Դ
		fs.close();
	}

	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 ���Єh��
		fs.delete(new Path("/test/����.png"), true);

		// 3 �ر���Դ
		fs.close();
	}

	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 �޸��ļ�����
		fs.rename(new Path("/test/�иּ���"), new Path("/test/����"));

		// 3 �ر���Դ
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 ��ȡ�ļ��б�
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println("·��: " + status.getPath());
			System.out.println("����: " + status.getPath().getName());
			System.out.println("����: " + status.getLen());
			System.out.println("Ȩ��: " + status.getPermission());
			System.out.println("����: " + status.getGroup());
			System.out.print("λ��: ");
			BlockLocation[] blockLocations = status.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for(String host: hosts) {
					System.out.print(host + " ");
				}
				System.out.println();
				System.out.println("========================================");
			}
		}
		// 3 �ر���Դ
		fs.close();		
	}
	
	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
		// 1 ��ȡ�ļ�ϵͳ
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 ��ȡ�ļ��б�
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
		for(FileStatus status : listStatus) {
			System.out.println(status.isFile() ? "�ļ�" : "Ŀ¼");
			System.out.println("·��: " + status.getPath());
			System.out.println("����: " + status.getPath().getName());
			System.out.println("����: " + status.getLen());
			System.out.println("Ȩ��: " + status.getPermission());
			System.out.println("����: " + status.getGroup());
			System.out.println("==================================");
		}
		// 3 �ر���Դ
		fs.close();		
	}
	
}
