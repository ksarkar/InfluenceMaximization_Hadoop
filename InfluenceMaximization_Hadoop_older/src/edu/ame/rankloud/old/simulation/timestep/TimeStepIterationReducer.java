package edu.ame.rankloud.old.simulation.timestep;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.simulation.init.NodeDataWritable;


public class TimeStepIterationReducer extends MapReduceBase 
	implements Reducer<Text, SimMapOutputWritable, Text, NodeDataWritable> {
	
	public static enum ActiveCounter {ACTIVATED_COUNT}

	public void reduce(Text key, 
					   Iterator<SimMapOutputWritable> values,
					   OutputCollector<Text, NodeDataWritable> output, 
					   Reporter reporter) throws IOException {
		
		double weightSum = 0;
		NodeDataWritable node = null;
		DoubleWritable w = null;

		while (values.hasNext()) {
			Writable instance = values.next().get();
			if (instance instanceof NodeDataWritable) {
				node = (NodeDataWritable) instance;
			}
			else {
				w = (DoubleWritable) instance;
				weightSum = weightSum + w.get();
			}
		}
		
		double threshold = node.getThreshold().get();
		
		if (weightSum >= threshold) {
			if (!node.getIsActive().get()) {
				// increment the global counter of activated users
				reporter.incrCounter(ActiveCounter.ACTIVATED_COUNT, 1);
				node.setIsActive(new BooleanWritable(true));
			}
		}
		
		output.collect(key, node);
	}

}
