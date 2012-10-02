package edu.ame.rankloud.model.ic;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.util.NeighborWritable;

/**
 * Mapper class for a time step.
 * in/data<Text, UWLTNodeData> -> <Text, TSMapOutput>
 */

public class TimeStepMapper extends MapReduceBase
	implements Mapper<Text, RPICNodeData, Text, TSMapOutput> {
	
	private Random random = new Random(System.currentTimeMillis());

	@Override
	public void map(Text key, 
					RPICNodeData value,
					OutputCollector<Text, TSMapOutput> output, 
					Reporter reporter) throws IOException {
		
		boolean state = ((BooleanWritable)value.getModelParam()).get();
		// check if the node is contiguous at the current time step
		if (state) {
			NeighborWritable[] neighbors = (NeighborWritable[])value.getNeighbors().toArray();
			
			double r = 0.0;
			for (NeighborWritable neighbor : neighbors) {
				r = random.nextDouble();
				if ( r <= neighbor.getWeight().get()) {
					output.collect(neighbor.getNodeId(), 
							   new TSMapOutput(new BooleanWritable(true)));		
				}
			}
			
			// the node will not be contiguous for the next time step
			value.setModelParam(new BooleanWritable(false));
		} 
		
		// send the node data structure
		output.collect(key, new TSMapOutput(value));
	}
}
