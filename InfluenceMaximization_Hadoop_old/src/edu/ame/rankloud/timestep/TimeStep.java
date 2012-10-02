package edu.ame.rankloud.timestep;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.TaskInfo;
import edu.ame.rankloud.util.Util;

public class TimeStep {
	
	public static enum ActiveCounter {ACTIVATED_COUNT}
	
	Model model;

	public TimeStep(Model model) {
		super();
		this.model = model;
	}
	
	public long run() throws IOException {
		TaskInfo ts = model.getTimeStep();
		
		JobConf conf = null;
		
		if (ts.isRequired()) { 
			conf = Util.getMapRedJobConf("timeStep",
					ts.getInputFormatClass(),
					ts.getMapperClass(),
					ts.getMapOutputKeyClass(),
					ts.getMapOutputValueClass(),
					ts.getNumReducer(),
					ts.getReducerClass(),
					ts.getOutputKeyClass(),
					ts.getOutputValueClass(),
					ts.getOutputFormatClass(),
					ts.getInputDirName(),
					ts.getOutputDirName());
		}
		
		System.out.println("Running time step: /in/data -> /out/data...");
		Counters counters = JobClient.runJob(conf).getCounters();
		return counters.getCounter(ActiveCounter.ACTIVATED_COUNT);
	}
}
