package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * TODO: test trying to delete root dir
 * should we delete everything under it?
 * 
 * @author aaron
 *
 */
public class TestDNNDelete extends TestCase {

	public void testSimpleDelete() {
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
		
		try {			
			Path p = new Path("/deletefile");
			FSDataOutputStream f = fs.create(p);
			f.close();
			
			assertTrue(fs.exists(p));
			
			
			fs.delete(p, false);
			
			assertFalse(fs.exists(p));
			
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testDeleteDir() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
	
		Path dir = new Path("/deletedir");
		try {
			fs.mkdirs(dir);
			
			assertTrue(fs.exists(dir));
			
		} catch (IOException e) {
		
			e.printStackTrace();
			assertTrue(false);
		}
		
		try {
			fs.delete(dir, false);
			
			assertFalse(fs.exists(dir));
			
		} catch (IOException e) {
			
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	public void testRecursiveDelete() {
		Configuration conf = new Configuration();
		FileSystem fs = null;
		
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
		
		// create some files and dirs
		try {
			fs.mkdirs(new Path("/dir1"));
			fs.mkdirs(new Path("/dir2"));
			
			FSDataOutputStream f = fs.create(new Path("/file1"));
			f.close();
			f = fs.create(new Path("/file2"));
			f.close();
			
			f = fs.create(new Path("/dir1/file1"));
			f.close();
			
			f = fs.create(new Path("/dir1/file2"));
			f.close();
			
			f = fs.create(new Path("/dir2/file1"));
			f.close();
			
			fs.mkdirs(new Path("/dir1/subdir1"));
			f = fs.create(new Path("/dir1/subdir1/file1"));
			f.close();
			
			f = fs.create(new Path("/dir1/subdir1/file2"));
			f.close();
			
			fs.mkdirs(new Path("/dir1/subdir2"));
			f = fs.create(new Path("/dir1/subdir2/file1"));
			f.close();
			
			f = fs.create(new Path("/dir1/subdir2/file2"));
			f.close();
			
			
		} catch (IOException e) {

			e.printStackTrace();
			assertTrue(false);
		}
		
		// delete /dir1/subdir2
		try {
			assertTrue(fs.exists(new Path("/dir1/subdir2")));
			assertTrue(fs.exists(new Path("/dir1/subdir2/file1")));
			assertTrue(fs.exists(new Path("/dir1/subdir2/file2")));
			
			fs.delete(new Path("/dir1/subdir2"), true);
			
			assertFalse(fs.exists(new Path("/dir1/subdir2")));
			assertFalse(fs.exists(new Path("/dir1/subdir2/file1")));
			assertFalse(fs.exists(new Path("/dir1/subdir2/file2")));
			
			// check nothing else was damaged
			assertTrue(fs.exists(new Path("/dir1/")));
			assertTrue(fs.exists(new Path("/dir1/file1")));
			assertTrue(fs.exists(new Path("/dir1/file2")));
			
			assertTrue(fs.exists(new Path("/dir1/subdir1")));
			assertTrue(fs.exists(new Path("/dir1/subdir1/file1")));
			assertTrue(fs.exists(new Path("/dir1/subdir1/file2")));
			
			assertTrue(fs.exists(new Path("/dir2")));
			assertTrue(fs.exists(new Path("/dir2/file1")));
			
			assertTrue(fs.exists(new Path("/file1/")));
			assertTrue(fs.exists(new Path("/file2/")));
			
			
			
		} catch (IOException e) {
		
			e.printStackTrace();
			
			assertTrue(false);
		}
		
		// delete /dir1
		try {
			assertTrue(fs.exists(new Path("/dir1")));
			assertTrue(fs.exists(new Path("/dir1/subdir1")));
			assertTrue(fs.exists(new Path("/dir1/subdir1/file1")));
			assertTrue(fs.exists(new Path("/dir1/subdir1/file2")));
			
			fs.delete(new Path("/dir1"), true);
			
			assertFalse(fs.exists(new Path("/dir1")));
			assertFalse(fs.exists(new Path("/dir1/subdir1")));
			assertFalse(fs.exists(new Path("/dir1/subdir1/file1")));
			assertFalse(fs.exists(new Path("/dir1/subdir1/file2")));
			
			// check nothing else was damaged
			assertTrue(fs.exists(new Path("/dir2/")));
			assertTrue(fs.exists(new Path("/file1/")));
			assertTrue(fs.exists(new Path("/file2/")));
			
		} catch (IOException e) {
		
			e.printStackTrace();
			
			assertTrue(false);
		}
		
	}
}
