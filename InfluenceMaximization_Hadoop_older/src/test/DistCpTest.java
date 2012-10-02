package test;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class DistCpTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		JobConf conf = new JobConf(DistCpTest.class);
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

/*
 	// test to check if delete works with non existing directories
		try {
			fs.delete(new Path("/user/hadoop-user/test1"), true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
*/		
		Path out = new Path("/user/hadoop-user/DistCp-test/out/*");
		
		FileStatus[] stats = null;
		try {
			stats = fs.globStatus(out);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Path[] paths = FileUtil.stat2Paths(stats);
		
		System.out.println("Following directories are found: ");
		for (Path path : paths) {
			System.out.println(path);
		}
		
		for (Path path : paths) {	
			try {
				fs.delete(path, true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		String[] command = {"/user/hadoop-user/DistCp-test/in/graph_bin", 
				"DistCp-test/out"};
		try {
			org.apache.hadoop.tools.DistCp.main(command);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
