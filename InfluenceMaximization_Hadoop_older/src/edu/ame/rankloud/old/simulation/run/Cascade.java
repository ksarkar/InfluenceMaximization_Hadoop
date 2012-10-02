package edu.ame.rankloud.old.simulation.run;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.old.simulation.init.NodeDataWritable;
import edu.ame.rankloud.old.simulation.timestep.SimMapOutputWritable;
import edu.ame.rankloud.old.simulation.timestep.TimeStepIterationMapper;
import edu.ame.rankloud.old.simulation.timestep.TimeStepIterationReducer;

public class Cascade {
	
	public static CascadeOutput runCascade(int numSeeds) {
		long total = numSeeds;
		long timeSteps = 0;
		
		System.out.println("Initializing run.");
		Cascade.initCascade();
		
		boolean run = true;
		
		while(run) {
			System.out.println("Running time step.");
			long count = Cascade.runTimeStep();
			System.out.println("Count of this timestep: " + count);
			if (count > 0) {
				total = total + count;
				run = true;
			} else {
				run = false;
			}
			
			System.out.println("Number of active users after this time step: " + total);
			
			// copy from out to in directory and delete the out data
			Cascade.recycleOutput();
			timeSteps++;
		}
		
		return new CascadeOutput(total, timeSteps);
	}
	
	
	// initialize the data for the first iteration
	private static void initCascade() {
		
		//JobConf conf = new JobConf(
		//		edu.ame.rankloud.simulation.run.Cascade.class);
		JobConf conf = new JobConf();
		conf.setJobName("initCascade");

		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(InitRunMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NodeDataWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		// delete the existing target output folder as well out/data folder too
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path("/user/hadoop-user/simulation/run/in/data"), true);
			fs.delete(new Path("/user/hadoop-user/simulation/run/out/data"), true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		FileInputFormat.addInputPath(conf, new Path("simulation/input/model"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/run/in/data"));

		long startTime = System.currentTimeMillis();
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
	}
	
	// run a single time step iteration
	private static long runTimeStep() {
		
		JobConf conf = new JobConf(Cascade.class);
		conf.setJobName("TimeStep");

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("simulation/run/in/data"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/run/out/data"));
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(TimeStepIterationMapper.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(SimMapOutputWritable.class);
		
		conf.setReducerClass(TimeStepIterationReducer.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NodeDataWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		
		Counters counters = null;
		RunningJob job = null;
		long startTime = System.currentTimeMillis();
		try {
			job = JobClient.runJob(conf);
			counters = job.getCounters();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
				
		return counters.getCounter(
				edu.ame.rankloud.old.simulation.timestep.TimeStepIterationReducer.ActiveCounter.ACTIVATED_COUNT);
	}
	
	// copy the data from the out to in directory for the next iteration
	// and do the clean up job
	private static void recycleOutput() {
		
		JobConf conf = new JobConf(Cascade.class);	
		FileSystem fs = null;
		Path in = new Path("/user/hadoop-user/simulation/run/in/*");
		FileStatus[] stats = null;
		
		// delete the in/data folder before copy
		System.out.println("Deleting old input files...");
		try {
			fs = FileSystem.get(conf);
			stats = fs.globStatus(in);
			Path[] paths = FileUtil.stat2Paths(stats);
			
			for (Path path : paths) {
				fs.delete(path, true);
				System.out.println("Deleted " + path);
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		// copy data
		// use the DistCp command to make use of map-reduce copy on HDFS
		String[] command = {"/user/hadoop-user/simulation/run/out/data", 
				"simulation/run/in"};
		System.out.println("Copying files from out to in...");
		try {
			runDistCp(command);
			System.out.println("Finished distcp");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// delete the out/data folder
		try {
			fs.delete(new Path("/user/hadoop-user/simulation/run/out/data"), true);
			System.out.println("Deleted simulation/run/out/data");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	public static void runDistCp(String[] args) throws Exception {
		JobConf conf = new JobConf(DistCp.class);
	    DistCp distcp = new DistCp(conf);
	    
	    long startTime = System.currentTimeMillis();
	    ToolRunner.run(distcp, args);
	    System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}
	
	public static void main(String[] args) {
		int numSeeds = 3;
		System.out.println("Initial number of seeds: " + numSeeds);
		
		long startTime = System.currentTimeMillis();
		CascadeOutput output = Cascade.runCascade(numSeeds);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.out.println("Final number of active users: " + output.getNumActive());
		System.out.println("Number of time steps: " + output.getNumTimeSteps());
	}

}
