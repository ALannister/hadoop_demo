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
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 创建目录
		fs.mkdirs(new Path("/test/中钢集团"));

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testCopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		conf.set("dfs.replication", "2");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 上传文件
		fs.copyFromLocalFile(new Path("C:\\Users\\HINOC\\Pictures\\无标题.png"), new Path("/test/动漫.png"));

		// 3 关闭资源
		fs.close();

		System.out.println("over");
	}

	@Test
	public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 执行下载操作
		// boolean delSrc 指是否将原文件删除
		// Path src 指要下载的文件路径
		// Path dst 指将文件下载到的路径
		// boolean useRawLocalFileSystem 是否开启文件校验
		fs.copyToLocalFile(false, new Path("/test/动漫.png"), new Path("C:\\Users\\HINOC\\Pictures\\动漫.png"), true);

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testDelete() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 執行刪除
		fs.delete(new Path("/test/动漫.png"), true);

		// 3 关闭资源
		fs.close();
	}

	@Test
	public void testRename() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 修改文件名称
		fs.rename(new Path("/test/中钢集团"), new Path("/test/北京"));

		// 3 关闭资源
		fs.close();
	}
	
	@Test
	public void testListFiles() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 获取文件列表
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		while(listFiles.hasNext()) {
			LocatedFileStatus status = listFiles.next();
			System.out.println("路径: " + status.getPath());
			System.out.println("名字: " + status.getPath().getName());
			System.out.println("长度: " + status.getLen());
			System.out.println("权限: " + status.getPermission());
			System.out.println("分组: " + status.getGroup());
			System.out.print("位置: ");
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
		// 3 关闭资源
		fs.close();		
	}
	
	@Test
	public void testListStatus() throws IOException, InterruptedException, URISyntaxException {
		// 1 获取文件系统
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "lxf");

		// 2 获取文件列表
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		
		for(FileStatus status : listStatus) {
			System.out.println(status.isFile() ? "文件" : "目录");
			System.out.println("路径: " + status.getPath());
			System.out.println("名字: " + status.getPath().getName());
			System.out.println("长度: " + status.getLen());
			System.out.println("权限: " + status.getPermission());
			System.out.println("分组: " + status.getGroup());
			System.out.println("==================================");
		}
		// 3 关闭资源
		fs.close();		
	}
	
}
