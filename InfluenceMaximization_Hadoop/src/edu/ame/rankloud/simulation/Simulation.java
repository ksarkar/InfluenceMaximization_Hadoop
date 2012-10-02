package edu.ame.rankloud.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import edu.ame.rankloud.cascade.Cascade;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.Util;

public class Simulation {
	private Model model;
	private int numRuns;
	private Cascade cascade;
	private String localSeedSetFile = DefaultFileName.LOCAL_SEED_SET_FILE;
	
	public Simulation(Model model, int numRuns, String localSeedSetFile) {
		super();
		this.model = model;
		this.numRuns = numRuns;
		if (localSeedSetFile != null)
			this.localSeedSetFile = localSeedSetFile;
		this.cascade = new Cascade(model);
	}

	public Simulation(Model model, int numRuns) {
		this(model, numRuns, null);
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

	public double run(String[] args) throws Exception {
		// mark the initial seeds active
		// TODO remove the getNumSeed() method and make initSimulation return the number
		// of seeds.
		this.initSimulation(args);
		int numSeed = this.getNumSeed();
		
		long sum = 0;
		for (int i = 0; i < numRuns; i++) {
			sum = sum + cascade.run(numSeed, args);
		}
		
		return (double) sum / numRuns;
	}
	
	public long[] singleRun(String[] args) throws Exception {
		model.initModel(args);
		this.initSimulation(args);
		int numSeed = this.getNumSeed();
		
		long[] result = new long[numRuns];
		for (int i = 0; i < numRuns; i++) {
			result[i] = cascade.run(numSeed, args);
		}
		
		return result;
	}
	
	private void initSimulation(String[] args) throws Exception {
		System.out.println("Initializng Simulation: /model -> /initSim...");
		ToolRunner.run(new InitSimTool(this.model, this.localSeedSetFile), args);
	}
	
	public class InitSimTool implements Tool {
		private Configuration conf;
		private Model model;
		private String localSeedSetFile;

		public InitSimTool(Model model, String localSeedSetFile) {
			super();
			this.model = model;
			this.localSeedSetFile = localSeedSetFile;
		}

		@Override
		public int run(String[] arg0) throws Exception {
			JobConf conf = Util.getMapRedJobConf(this.getClass(),
												this.getConf(),
												"initSimulation", 
												this.model.getInputFormatClass(),
												InitSimMapper.class,
												null,
												null,
												0, // no reducer
												null, 
												this.model.getOutputKeyClass(),
												this.model.getOutputValueClass(),
												this.model.getOutputFormatClass(),
												DefaultFileName.HDFS_INPUT_MODEL, 
												DefaultFileName.HDFS_INPUT_INIT_SIM);

			this.cacheSeedList(conf);
	
			JobClient.runJob(conf);
			return 0;
		}
		
		
		private void cacheSeedList(JobConf conf) throws IOException {
		    FileSystem fs = FileSystem.get(conf);
		    Path hdfsPath = new Path(DefaultFileName.HDFS_INPUT_SEED_LIST);
		    Path localPath = new Path(this.localSeedSetFile);

		    // upload the file to hdfs. Overwrite any existing copy.
		    fs.copyFromLocalFile(false, true, localPath, hdfsPath);

		    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
		}

		@Override
		public Configuration getConf() {
			return this.conf;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
		}
		
	}

	public int getNumSeed() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.localSeedSetFile));
		int seeds = 0;
		try {
			while (reader.readLine() != null) seeds++;
		} finally {
			reader.close();
		}
		
		return seeds;
	}
	

}
