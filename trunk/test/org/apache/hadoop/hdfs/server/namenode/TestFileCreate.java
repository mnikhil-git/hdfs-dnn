package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFileCreate extends TestCase {
	public void test() {

		FileSystem fs = null;
		Configuration hdfsConf = null;

		try {

			hdfsConf = new Configuration();
			//DFSClient client = new DFSClient(hdfsConf);

			//OutputStream out = client.create("/myfile", false);
			//out.write("mydata".getBytes());
			//out.close();

			fs = FileSystem.get(hdfsConf);
		


			FSDataOutputStream out = fs.create(new Path("/testfile7"));

			for(int i=0; i < 6000000; i++)
				out.writeUTF("This is a test. ");

			out.close();

			//FSDataInputStream stream = fs.open(new Path("/testfile3"));
			//String line = stream.readUTF();
			//System.out.println("read: " + line);

		
			FileStatus stat = fs.getFileStatus(new Path("/testfile7"));
			assertTrue(stat.getLen() == 96000000);
			System.out.println("len: " + stat.getLen() +
					" modtime: " + stat.getModificationTime() +
					" blocksize: " + stat.getBlockSize() +
					" perm: " + stat.getPermission().toString() + 
					" replication: " + stat.getReplication());
					
		} catch(IOException e) {
			e.printStackTrace();
			assertTrue(false);
		}

	}
}
