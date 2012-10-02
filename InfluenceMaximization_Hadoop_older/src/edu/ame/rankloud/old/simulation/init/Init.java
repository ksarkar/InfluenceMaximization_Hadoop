package edu.ame.rankloud.old.simulation.init;

import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


public class Init {
	
	public static final String LOCAL_SEED_LIST = 
		"./data/seed_list.txt";
	
	public static final String HDFS_SEED_LIST = 
		"/data/seed_list.txt";

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(Init.class);
		
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path("/user/hadoop-user/simulation/input/model"), true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("simulation/input/graph"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/input/model"));
		
		//add side data file to distributed cache
		try {
			cacheSeedList(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(InitMapper.class);
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(InitMapOutWritable.class);
		
		//conf.setNumReduceTasks(0);
		conf.setReducerClass(InitReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NodeDataWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		client.setConf(conf);
		long startTime = System.currentTimeMillis();
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}
	
	static void cacheSeedList(JobConf conf) throws IOException {
	    FileSystem fs = FileSystem.get(conf);
	    Path hdfsPath = new Path(HDFS_SEED_LIST);
	    Path localPath = new Path(LOCAL_SEED_LIST);

	    // upload the file to hdfs. Overwrite any existing copy.
	    fs.copyFromLocalFile(false, true, localPath, hdfsPath);

	    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	  }

}
