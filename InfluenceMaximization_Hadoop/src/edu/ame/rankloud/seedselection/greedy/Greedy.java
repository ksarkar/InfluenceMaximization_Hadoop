package edu.ame.rankloud.seedselection.greedy;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.common.OptResult;
import edu.ame.rankloud.seedselection.SeedSelectionStrategy;
import edu.ame.rankloud.simulation.Simulation;


public class Greedy implements SeedSelectionStrategy {
	
	protected Model model;
	protected Simulation simulation;
	protected int numSeeds;
	protected String localSeedSetFile = DefaultFileName.LOCAL_SEED_SET_FILE;
	protected String hdfsNodeListFile = DefaultFileName.HDFS_INPUT_NODELIST;
	
		
	public Greedy(Model model, int numSeeds, int numRuns) {
		this(model, numSeeds, numRuns, null, null);
	}
	
	

	public Greedy(Model model, 
				  int numSeeds, 
				  int numRuns, 
				  String localSeedSetFile,
				  String hdfsNodeListFile) {
		super();
		this.model = model;
		this.numSeeds = numSeeds;		
		if (localSeedSetFile != null) {
			this.localSeedSetFile = localSeedSetFile;
			this.simulation = new Simulation(model, numRuns, localSeedSetFile);
		}
		else {
			this.simulation = new Simulation(model, numRuns);
		}
		if (hdfsNodeListFile != null)
			simulation.setLocalSeedSetFile(localSeedSetFile);
	}



	public OptResult run(String[] args) throws Exception {
		
		// initialize the model
		model.initModel(args);
		
		// write the list of all nodes in nodeList
		this.makeNodeList(args);
		
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
					double sigma = simulation.run(args);
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
	 * Makes a nodeList of all nodes 
	 * 
	 * @return number of nodes in the graph
	 * @throws Exception 
	 */
	
	protected long makeNodeList(String[] args) throws Exception {
		System.out.println("Initializng nodeList: /model -> /nodeList...");
		return ToolRunner.run(new MakeNodeListTool(this.model, this.hdfsNodeListFile), args);	
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

}
