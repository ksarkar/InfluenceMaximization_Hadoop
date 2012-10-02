package edu.ame.rankloud.old.optimization;

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
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import edu.ame.rankloud.old.simulation.init.Simulation;

public class Optimization {
	
	public static OptResult optimize(int numSeed) throws IOException {
		
		int runs = 5;
		
		System.out.println("Initializing the model: ");
		initModel();
		Set<String> seedSet = new HashSet<String>();
		int k = 0;
		double Max = 0;
		
		while (k < numSeed) {
			System.out.println("Seed set size: " + k);
			String max = null;
			Max = 0;
			// write the current seed set to the file
			long lastSeedPos = writeSeedFile(seedSet);
			
			NodeList nl = new NodeList();
			
			StringBuilder nextNode = new StringBuilder();
			while (nl.next(nextNode)) {
				if (!seedSet.contains(nextNode.toString())) {
					appendSeedFile(lastSeedPos, nextNode.toString());
					double sigma = Simulation.expectedSpread(runs, null, k + 1);
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

	private static void initModel() throws IOException {
		
		JobConf conf = new JobConf(Optimization.class);
		conf.setJobName("InitModel");
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(InitModelMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		// delete the existing target output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path("/user/hadoop-user/simulation/input/nodeList"), true);
		

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("simulation/input/graph"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/input/nodeList"));
		
		long startTime = System.currentTimeMillis();
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
	}
	
	private static long writeSeedFile(Set<String> seedSet) throws IOException {
		
		// first delete the existing seed file
		File file = new File("./data/seed_list.txt");
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
	
	private static void appendSeedFile(long pos, String newSeed) throws IOException {
		RandomAccessFile f = null;
		f = new RandomAccessFile("./data/seed_list.txt", "rw");
		
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
	
	// a wrapper class around the SequenceFileReader
	private static class NodeList {
		JobConf conf;
		FileSystem fs;
		SequenceFile.Reader reader = null;
		Writable key;
		Writable value;
		int openFiles;
		Path[] paths;
		
		NodeList() throws IOException {
			this.conf = new JobConf(NodeList.class);
			Path nodeList = new Path("/user/hadoop-user/simulation/input/nodeList/part*");
			
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
		boolean next(StringBuilder node) throws IOException {
			
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
		
	}
	
	public static void main(String[] args) throws IOException {
		
		int seedSetSize = 2;
		
		System.out.println("Optimization for seed set size " + seedSetSize + " starting: ");
		long startTime = System.currentTimeMillis();
		OptResult opt = optimize(2);
		System.out.println("Optimization completed in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.out.println("Seed set for maximizing contagion: ");
		Iterator<String> seeds = opt.getSeedSet().iterator();
		while (seeds.hasNext()) {
			System.out.println(seeds.next());
		}
		System.out.println("Maximum achievable spread: " + opt.getSpread());
	}

}
