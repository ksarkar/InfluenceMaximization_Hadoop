package edu.ame.rankloud.seedselection.greedy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.TaskInfo;
import edu.ame.rankloud.seedselection.DefaultFileName;
import edu.ame.rankloud.seedselection.OptResult;
import edu.ame.rankloud.seedselection.SeedSelectionStrategy;
import edu.ame.rankloud.simulation.Simulation;
import edu.ame.rankloud.util.Util;


public class Greedy implements SeedSelectionStrategy {
	
	protected Model model;
	protected Simulation simulation;
	protected int numSeeds;
	protected String localSeedSetFile = DefaultFileName.LOCAL_SEED_SET_FILE;
	protected String hdfsNodeListFile = DefaultFileName.HDFS_INPUT_NODELIST;
	
		
	public Greedy(int numSeeds, Model model, Simulation simulation) {
		super();
		this.numSeeds = numSeeds;
		this.model = model;
		this.simulation = simulation;
		this.simulation.setModel(model);
		simulation.setLocalSeedSetFile(this.localSeedSetFile);
	}
	
	

	public Greedy(int numSeeds, Model model, Simulation simulation, String localSeedSetFile) {
		super();
		this.numSeeds = numSeeds;
		this.model = model;
		this.simulation = simulation;
		this.simulation.setModel(model);
		this.localSeedSetFile = localSeedSetFile;
		simulation.setLocalSeedSetFile(localSeedSetFile);
	}



	public OptResult run() throws Exception {
		
		// initialize the model
		// write the list of all nodes in nodeList
		this.initModel();
		
		Set<String> seedSet = new HashSet<String>();
		int k = 0;
		double Max = 0;
		
		while (k < this.numSeeds) {
			String max = null;
			Max = 0;
			
			// write the current seed set to the local seed set file
			long lastSeedPos = this.writeSeedFile(seedSet);
			
			// list abstraction for reading from the nodeList file
			// sequentially
			NodeList nl = new NodeList(this.hdfsNodeListFile);
			
			StringBuilder nextNode = new StringBuilder();
			while (nl.next(nextNode)) {
				String nextNodeS = nextNode.toString();
				if (!seedSet.contains(nextNodeS)) {
					// delete the last seed and add this seed
					this.appendSeedFile(lastSeedPos, nextNodeS);
					double sigma = simulation.run();
					if (sigma > Max) {
						Max = sigma;
						max = nextNode.toString();
					}
				}
			}
			
			seedSet.add(max);
			k++;
			
		}
		
		return new OptResult(seedSet, Max);
		
	}
	
	/**
	 * Initializes the model; makes a nodeList of all nodes 
	 * 
	 * @return number of nodes in the graph
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	protected long initModel() throws IOException {
		// initialize the model; graph -> model
		TaskInfo init = model.getInitModel();
		TaskInfo cascade = model.getInitCascade();
		
		JobConf conf = null;
		
		if (init.isRequired()) {
			conf = Util.getMapRedJobConf("initModel",
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
		
		// initialize the nodeList; model -> nodeList
		System.out.println("Initializng nodeList: /model -> /nodeList...");
		conf = Util.getMapRedJobConf("initNodeList", 
									 cascade.getInputFormatClass(), 
									 InitNodeListMapper.class, 
									 null, 
									 null, 
									 0, 
									 null, 
									 Text.class, 
									 Text.class, 
									 SequenceFileOutputFormat.class, 
									 DefaultFileName.HDFS_INPUT_MODEL, 
									 DefaultFileName.HDFS_INPUT_NODELIST);
		
		Counters counters = JobClient.runJob(conf).getCounters();
		return counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 
									0, 
									"MAP_INPUT_RECORDS").getCounter();
	}
	
	protected long writeSeedFile(Set<String> seedSet) throws IOException {
		
		// first delete the existing seed file
		File file = new File(this.localSeedSetFile);
		file.delete();
		
		// now create a random access file for writing the seed set
		RandomAccessFile f = null;

		f = new RandomAccessFile(file, "rw");
		
		long pos = 0;
		Iterator<String> seeds = seedSet.iterator();
		
		try {
			while(seeds.hasNext()) {
	        	f.writeBytes(seeds.next() + "\n");
	        }
			
			pos = f.length();
		} finally {
			if (f != null) {
				f.close();				
			}
		}
		
		return pos;
	}
	
	protected void appendSeedFile(long pos, String newSeed) throws IOException {
		RandomAccessFile f = null;
		f = new RandomAccessFile(this.localSeedSetFile, "rw");
		
		try {
			f.seek(pos);
			long end = f.length();
			for (long i = pos; i < end; i++) {
				f.writeByte(' ');
			}
			f.seek(pos);
			f.writeBytes(newSeed + "\n");
		} finally {
			if (f != null) {
				f.close();
			}
		}
		
	}
	
	// List abstraction for a sequence file reader
	protected static class NodeList {
		private JobConf conf;
		private FileSystem fs;
		private SequenceFile.Reader reader = null;
		private Writable key;
		private Writable value;
		private int openFiles;
		private Path[] paths;
		
		public NodeList(String nodeListDir) throws IOException {
			this.conf = new JobConf(NodeList.class);
			Path nodeList = new Path( nodeListDir + "/part*");
			
			this.fs = FileSystem.get(conf);
			this.paths = FileUtil.stat2Paths(fs.globStatus(nodeList));
			this.openFiles = paths.length;
			
			if (this.openFiles > 0) {
				this.reader = new SequenceFile.Reader(fs, paths[0], conf);
				this.key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
				this.value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			}
	
		}
		
		// Recursive function for iterating over multiple files
		public boolean next(StringBuilder node) throws IOException {
			
			boolean next = false;
			
			try {
				next = reader.next(key, value);
			} catch (IOException e) {
				if (reader != null) {
					IOUtils.closeStream(reader);
					this.openFiles--;
				}
				throw e;
			}
			
			if (next) {
				String sKey = key.toString();
				node.delete(0, node.length());
				node.append(sKey);
			}
			
			else { // current file is finished
				// close the current file
				if (reader != null) {
					IOUtils.closeStream(reader);
					this.openFiles--;
				}
				// open next file; if one exists
				if (this.openFiles > 0) {
					this.reader = new SequenceFile.Reader(fs, paths[paths.length - openFiles], conf);
					next = next(node);
				}
			}
			
			return next;
		}
		
		public void close() {
			if (reader != null) {
				IOUtils.closeStream(reader);
				this.openFiles--;
			}
		}
		
	}

}
