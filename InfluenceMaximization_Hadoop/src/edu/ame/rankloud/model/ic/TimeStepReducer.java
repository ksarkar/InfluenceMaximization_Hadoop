package edu.ame.rankloud.model.ic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
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
	implements Reducer<Text, TSMapOutput, Text, RPICNodeData> {
	
	//public static enum ActiveCounter {ACTIVATED_COUNT}

	@Override
	public void reduce(Text key, 
					   Iterator<TSMapOutput> values,
					   OutputCollector<Text, RPICNodeData> output, 
					   Reporter reporter) throws IOException {
	
		boolean isContiguous = false;
		boolean temp = false;
		RPICNodeData nodeData = null;

		while (values.hasNext()) {
			Writable instance = values.next().get();
			if (instance instanceof RPICNodeData) {
				nodeData = (RPICNodeData) instance;
			}
			else {
				temp = ((BooleanWritable) instance).get();
				if (temp)
					isContiguous = true;
			}
		}
		
		if (isContiguous) {
			if (!nodeData.getIsActive().get()) {
				// increment the global counter of activated users
				reporter.incrCounter(ActiveCounter.ACTIVATED_COUNT, 1);
				nodeData.setIsActive(new BooleanWritable(true));
			}
		}
		
		output.collect(key, nodeData);
	}
}
