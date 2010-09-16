package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDistributedNamenodeProxy extends TestCase {

	public void test() {
		
		FileSystem fs = null;
		Configuration hdfsConf = null;
		
		try {
			
			 hdfsConf = new Configuration();
			
			 fs = FileSystem.get(hdfsConf);
		}
		catch(IOException e) {
			assertTrue(false);
		}
		
		
		
		long bytesRead = 0L;
		
		try {
			FSDataInputStream in = fs.open(new Path("/testfile7"));
			int i;
			String line = null;
			
			for(i=0; (line = in.readUTF()) != null; i++) {
				System.out.println(line.length() + " " + bytesRead);
				bytesRead += line.length();
			}
				//System.out.println(line);
			System.out.println(bytesRead);
		}
		catch(IOException e) {
			System.out.println(bytesRead);
			e.printStackTrace();
			assertTrue(false);
		} 
	}
}
