package edu.ame.rankloud.model.lt;

import java.io.IOException;

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
	implements Mapper<Text, UWLTNodeData, Text, TSMapOutput> {

	@Override
	public void map(Text key, 
					UWLTNodeData value,
					OutputCollector<Text, TSMapOutput> output, 
					Reporter reporter) throws IOException {
		
		boolean state = value.getIsActive().get();
		if (state) {
			NeighborWritable[] neighbors = (NeighborWritable[])value.getNeighbors().toArray();
			for (NeighborWritable neighbor : neighbors) {
				output.collect(neighbor.getNodeId(), 
							   new TSMapOutput(neighbor.getWeight()));
			}
			
		} // no need to send any weights if node not active
		
		// send the node data structure
		output.collect(key, new TSMapOutput(value));
	}
}
