package edu.ame.rankloud.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.NodeDataWritable;

public class InitSimMapper<K extends WritableComparable<K>, V extends NodeDataWritable>
	extends MapReduceBase 
	implements Mapper<K, V, K, V> {
	
	private Set<String> seeds = null;
	
	public void configure(JobConf conf) {
	    try {
	      String seedCacheName = new Path(DefaultFileName.HDFS_INPUT_SEED_LIST).getName();
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

	public void map(K key, 
					V value,
					OutputCollector<K, V> output, 
					Reporter reporter) throws IOException {
		
		boolean isActive = seeds.contains(key.toString())? true : false;
		
		value.setIsActive(new BooleanWritable(isActive));
		
		output.collect(key, value);
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

}
