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

import edu.ame.rankloud.old.simulation.run.Cascade;
import edu.ame.rankloud.old.simulation.run.CascadeOutput;

public class Simulation {
	
	public static String LOCAL_SEED_LIST = 
		"./data/seed_list.txt";
	
	public static final String HDFS_SEED_LIST = 
		"/data/seed_list.txt";
	
	public static CascadeOutput[] simulate(int numRuns, String seed_file_name, int numSeeds) {
		
		System.out.println("Initializing Cascade...");
		Simulation.initModel(seed_file_name);
		
		CascadeOutput[] out = new CascadeOutput[numRuns];
		for (int i = 0; i < numRuns; i++) {
			System.out.println("Running one cascade...");
			out[i] = Cascade.runCascade(numSeeds);
		}
		
		return out;
	}
	
	public static double expectedSpread(int numRuns, String seed_file_name, int numSeeds) {
		
		long startTime = System.currentTimeMillis();
		CascadeOutput[] out = Simulation.simulate(numRuns, null, numSeeds);
		System.out.println("Simulation Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		long sum = 0;
		for (CascadeOutput c : out) {
			sum = sum + c.getNumActive();
		}
		
		return (double) sum / numRuns;
	}
 	

	private static void initModel(String seed_file_name) {
		
		if (seed_file_name != null) 
			LOCAL_SEED_LIST = seed_file_name;
		
		JobConf conf = new JobConf(Simulation.class);
		conf.setJobName("InitModel");

		
		// add side data file to distributed cache
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
		
		conf.setReducerClass(InitReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NodeDataWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		// delete the existing target output folder
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
		
		long startTime = System.currentTimeMillis();
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}
	
	private static void cacheSeedList(JobConf conf) throws IOException {
	    FileSystem fs = FileSystem.get(conf);
	    Path hdfsPath = new Path(HDFS_SEED_LIST);
	    Path localPath = new Path(LOCAL_SEED_LIST);

	    // upload the file to hdfs. Overwrite any existing copy.
	    fs.copyFromLocalFile(false, true, localPath, hdfsPath);

	    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}
	
	public static void main(String[] args) {
		int runs = 5;
		
		System.out.println("Starting simulation...");
		
		long startTime = System.currentTimeMillis();
		CascadeOutput[] out = Simulation.simulate(runs, null, 1);
		System.out.println("Simulation Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		long sum = 0;
		for (CascadeOutput c : out) {
			sum = sum + c.getNumActive();
		}
		
		System.out.println("Expected number of adopters: " + ((float) sum / runs));
	}


}
