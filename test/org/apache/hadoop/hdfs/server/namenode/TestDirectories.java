package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestDirectories extends TestCase {

	public void testMkDir() {
		Configuration conf = new Configuration();

		FileSystem fs = null;

		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}

		try {

			fs.mkdirs(new Path("/dir"));

			assertTrue(fs.getFileStatus(new Path("/dir")).isDir());

			fs.mkdirs(new Path("/dir/subdir1"));

			fs.mkdirs(new Path("/dir/subdir2"));

			fs.mkdirs(new Path("/dir/subdir1/subsubdir1"));

			FSDataOutputStream f = fs.create(new Path("/dir/file1"));
			f.close();

			assertFalse(fs.getFileStatus(new Path("/dir/file1")).isDir());

			f = fs.create(new Path("/dir/file2"));
			f.close();

			f = fs.create(new Path("/dir/subdir1/file1"));
			f.close();

			f = fs.create(new Path("/dir/subdir1/file2"));
			f.close();

			f = fs.create(new Path("/dir/subdir1/subsubdir1/file1"));
			f.close();

			f = fs.create(new Path("/dir/subdir1/subsubdir1/file2"));
			f.close();


			FileStatus[] files = fs.listStatus(new Path("/dir"));
			for(FileStatus stat : files) {
				System.out.println(
						stat.getPath() + "\t" +
						stat.getModificationTime() + " \t" +
						stat.getReplication() + "\t"
				);
			}

		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}

	}
	
	
	
	public void testFailures() {
		
		Configuration conf = new Configuration();

		FileSystem fs = null;

		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
		
		// ensure creating a file in nonexistent directory fails
		try {
			fs.create(new Path("/nonexistent/file1"));
			assertTrue(false);
		} catch (IOException e2) {

		}

		// create with a file as the parent should fail
		try {
			fs.create(new Path("/dir/file1/file2"));
			assertTrue(false);
		}
		catch(IOException e1) {
			
		}
		
		// create existent directory should fail
		try {
			fs.mkdirs(new Path("/dir"));
			assertTrue(false);
		} catch (IOException e1) {

		}

		// create a file that is a directory should fail
		try {
			fs.create(new Path("/dir"));
			assertTrue(false);
		} catch (IOException e) {

		}

		// list dir on file should fail
		try {
			fs.listStatus(new Path("/dir/file1"));
			assertTrue(false);
		} catch (IOException e) {
			
		}
	}
	
}
