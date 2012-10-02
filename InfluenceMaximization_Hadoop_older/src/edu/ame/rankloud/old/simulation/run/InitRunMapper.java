package edu.ame.rankloud.old.simulation.run;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.simulation.init.NodeDataWritable;

public class InitRunMapper extends MapReduceBase 
	implements Mapper<Text, NodeDataWritable, Text, NodeDataWritable> {
	
	private Random rand = new Random(System.currentTimeMillis());

	public void map(Text key, 
					NodeDataWritable value,
					OutputCollector<Text, NodeDataWritable> output, 
					Reporter reporter) throws IOException {
		
		value.setThreshold(new DoubleWritable(rand.nextDouble()));
		output.collect(key, value);
	}

}
