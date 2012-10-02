package edu.ame.rankloud.model.lt;

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

import edu.ame.rankloud.model.AbstractModel.ActiveCounter;

/**
 * Reducer class for a time step
 * <Text, TSMapOutput> -> run/out/data<Text, UWLTNodeData>
 */
public class TimeStepReducer extends MapReduceBase
	implements Reducer<Text, TSMapOutput, Text, UWLTNodeData> {
	
	//public static enum ActiveCounter {ACTIVATED_COUNT}

	@Override
	public void reduce(Text key, 
					   Iterator<TSMapOutput> values,
					   OutputCollector<Text, UWLTNodeData> output, 
					   Reporter reporter) throws IOException {
	
		double weightSum = 0;
		UWLTNodeData nodeData = null;
		DoubleWritable w = null;

		while (values.hasNext()) {
			Writable instance = values.next().get();
			if (instance instanceof UWLTNodeData) {
				nodeData = (UWLTNodeData) instance;
			}
			else {
				w = (DoubleWritable) instance;
				weightSum = weightSum + w.get();
			}
		}
		
		double threshold = ((DoubleWritable) nodeData.getModelParam()).get();
		
		if (weightSum >= threshold) {
			if (!nodeData.getIsActive().get()) {
				// increment the global counter of activated users
				reporter.incrCounter(ActiveCounter.ACTIVATED_COUNT, 1);
				nodeData.setIsActive(new BooleanWritable(true));
			}
		}
		
		output.collect(key, nodeData);
	}
}
