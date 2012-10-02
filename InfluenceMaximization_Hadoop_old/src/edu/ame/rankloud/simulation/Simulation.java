package edu.ame.rankloud.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


import edu.ame.rankloud.cascade.Cascade;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.TaskInfo;
import edu.ame.rankloud.seedselection.DefaultFileName;
import edu.ame.rankloud.util.Util;

public class Simulation {
	private int numRuns;
	private Model model;
	private Cascade cascade;
	private String localSeedSetFile = DefaultFileName.LOCAL_SEED_SET_FILE;
	
	public Simulation(int numRuns, Model model, String localSeedSetFile) {
		super();
		this.numRuns = numRuns;
		this.model = model;
		this.localSeedSetFile = localSeedSetFile;
		this.cascade = new Cascade(model);
	}

	public Simulation(int numRuns, Model model) {
		super();
		this.numRuns = numRuns;
		this.model = model;
		this.cascade = new Cascade(model);
	}

	public String getLocalSeedSetFile() {
		return localSeedSetFile;
	}

	public void setLocalSeedSetFile(String localSeedSetFile) {
		this.localSeedSetFile = localSeedSetFile;
	}
	
	
	
	
	public Model getModel() {
		return model;
	}

	public void setModel(Model model) {
		this.model = model;
	}

	public double run() throws Exception {
		// mark the initial seeds active
		this.initSimulation();
		long numSeed = this.getNumSeed();
		
		long sum = 0;
		for (int i = 0; i < numRuns; i++) {
			sum = sum + cascade.run(numSeed);
		}
		
		return (double) sum / numRuns;
	}
	
	public long[] singleRun() throws Exception {
		this.initModel();
		this.initSimulation();
		
		long numSeed = this.getNumSeed();
		
		long[] result = new long[numRuns];
		for (int i = 0; i < numRuns; i++) {
			result[i] = cascade.run(numSeed);
		}
		
		return result;
	}
	
	public long[] cascadeSingleRun() throws Exception {
		 this.initModel();
		 this.initSimulation();
		 
		 long numSeed = this.getNumSeed();
		 
		 long[] result = cascade.singleRun(numSeed);
		 return result;
	}
	
	
	private void initSimulation() throws IOException {
		TaskInfo init = model.getInitCascade();
		
		JobConf conf = Util.getMapRedJobConf("initSimulation", 
												init.getInputFormatClass(),
												InitSimMapper.class,
												null,
												null,
												0, // no reducer
												null, 
												init.getOutputKeyClass(),
												init.getOutputValueClass(),
												init.getOutputFormatClass(),
												DefaultFileName.HDFS_INPUT_MODEL, 
												DefaultFileName.HDFS_INPUT_INIT_SIM);
		
		this.cacheSeedList(conf);
		
		System.out.println("Initializng Simulation: /model -> /initSim...");
		JobClient.runJob(conf);
	}
	
	private void cacheSeedList(JobConf conf) throws IOException {
	    FileSystem fs = FileSystem.get(conf);
	    Path hdfsPath = new Path(DefaultFileName.HDFS_INPUT_SEED_LIST);
	    Path localPath = new Path(this.localSeedSetFile);

	    // upload the file to hdfs. Overwrite any existing copy.
	    fs.copyFromLocalFile(false, true, localPath, hdfsPath);

	    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}
	
	private void initModel() throws IOException {
		TaskInfo init = model.getInitModel();
		
		if (init.isRequired()) {
			JobConf conf = Util.getMapRedJobConf("initModel",
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
			System.out.println("Initializng model: /graph -> /model...");
			JobClient.runJob(conf);
		}
	}
	
	
	public long getNumSeed() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.localSeedSetFile));
		long seeds = 0;
		try {
			while (reader.readLine() != null) seeds++;
		} finally {
			reader.close();
		}
		
		return seeds;
	}
	

}
