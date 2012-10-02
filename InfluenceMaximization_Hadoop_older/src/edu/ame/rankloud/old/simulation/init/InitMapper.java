package edu.ame.rankloud.old.simulation.init;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.util.TextArrayWritable;

public class InitMapper extends MapReduceBase 
	implements Mapper<Text, TextArrayWritable, Text, InitMapOutWritable> {
	
	//private Random random = new Random(System.currentTimeMillis());
	private Set<String> seeds = null;
	
	
	// test method
	public void setSeed(Set<String> seeds) {
		this.seeds = seeds;
	}
	
	public void configure(JobConf conf) {
	    try {
	      String seedCacheName = new Path(Init.HDFS_SEED_LIST).getName();
	      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
	      if (null != cacheFiles && cacheFiles.length > 0) {
	        for (Path cachePath : cacheFiles) {
	          if (cachePath.getName().equals(seedCacheName)) {
	            loadSeedList(cachePath);
	            break;
	          }
	        }
	      }
	    } catch (IOException ioe) {
	      System.err.println("IOException reading from distributed cache");
	      System.err.println(ioe.toString());
	    }
	  }
	
	void loadSeedList(Path cachePath) throws IOException {
	    // note use of regular java.io methods here - this is a local file now
	    BufferedReader wordReader = new BufferedReader(
	        new FileReader(cachePath.toString()));
	    try {
	      String line;
	      this.seeds = new HashSet<String>();
	      while ((line = wordReader.readLine()) != null) {
	        this.seeds.add(line);
	      }
	    } finally {
	      wordReader.close();
	    }
	  }
	
	public void map(Text key, 
					TextArrayWritable value,
					OutputCollector<Text, InitMapOutWritable> output, 
					Reporter reporter) throws IOException {
		
		int numNeighbors = value.get().length;
		double weight = (double)1 / numNeighbors;
		
		Text[] neighbors = (Text[]) value.toArray();
		
		InitMapOutWritable me = new InitMapOutWritable(new NeighborWritable(key, new DoubleWritable(weight)));
		for (Text neighbor : neighbors) {
			output.collect(neighbor, me);
		}
		
		// send threshold
		//output.collect(key, new InitMapOutWritable(new DoubleWritable(random.nextDouble())));
		
		// send state
		boolean isActive = seeds.contains(key.toString())? true : false;
		output.collect(key, new InitMapOutWritable(new BooleanWritable(isActive)));
		
	}	
}
