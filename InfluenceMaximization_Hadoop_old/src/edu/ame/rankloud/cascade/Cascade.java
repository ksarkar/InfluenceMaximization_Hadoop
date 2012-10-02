package edu.ame.rankloud.cascade;

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.TaskInfo;
import edu.ame.rankloud.seedselection.DefaultFileName;
import edu.ame.rankloud.timestep.TimeStep;
import edu.ame.rankloud.util.Util;

public class Cascade {
	Model model;
	TimeStep timeStep;

	public Cascade(Model model) {
		super();
		this.model = model;
		this.timeStep = new TimeStep(model);
	}
	
	public long run(long numSeed) throws Exception {
		long[] result = this.singleRun(numSeed);
		return result[0];
	}
	
	public long[] singleRun(long numSeed) throws Exception {
		long total = numSeed;
		long timeSteps = 0;
		
		this.initCascade();
		
		boolean run = true;
		
		while(run) {
			long count = timeStep.run();
			
			if (count > 0) {
				total = total + count;
				run = true;
			} else {
				run = false;
			}
			
			this.recycleOutput();
			timeSteps++;
		}
		
		long[] result = new long[2];
		result[0] = total;
		result[1] = timeSteps;
		return result;
	}
	
	private void initCascade() throws IOException {
		TaskInfo init = model.getInitCascade();
		
		if (init.isRequired()) {
			JobConf conf = Util.getMapRedJobConf("initCascade",
					init.getInputFormatClass(),
					init.getMapperClass(),
					init.getMapOutputKeyClass(),
					init.getMapOutputValueClass(),
					init.getNumReducer(),
					init.getReducerClass(),
					init.getOutputKeyClass(),
					init.getOutputValueClass(),
					init.getOutputFormatClass(),
					init.getInputDirName(),
					init.getOutputDirName());
			
			System.out.println("Initializng Cascade: /initSim -> /in/data...");
			JobClient.runJob(conf);
		}
	}
	
	// delete files in run/in/ folder
	// copy the data from the out to in directory for the next iteration
	// clean up run/out directory
	private void recycleOutput() throws Exception {
		
		JobConf conf = new JobConf(Cascade.class);	
		FileSystem fs = null;
		Path in = new Path(DefaultFileName.HDFS_RUN_IN +  "/*");
		FileStatus[] stats = null;
		
		// delete the in/data folder before copy
	
		fs = FileSystem.get(conf);
		stats = fs.globStatus(in);
		Path[] paths = FileUtil.stat2Paths(stats);
		
		for (Path path : paths) {
			fs.delete(path, true);
		}
		
		// copy data from out/data to in/
		// use the DistCp command to make use of map-reduce copy on HDFS
		String[] command = {DefaultFileName.HDFS_RUN_OUT + "/data", 
				DefaultFileName.HDFS_RUN_IN};
		
		System.out.println("Recycling data: /out/data -> /in/data...");
		this.runDistCp(command);
				
		
		// delete the out/data folder
		fs.delete(new Path(DefaultFileName.HDFS_RUN_OUT_DATA), true);
	
	}
	
	public void runDistCp(String[] args) throws Exception {
		JobConf conf = new JobConf(DistCp.class);
	    DistCp distcp = new DistCp(conf);
	    
	    ToolRunner.run(distcp, args);
	}

}
