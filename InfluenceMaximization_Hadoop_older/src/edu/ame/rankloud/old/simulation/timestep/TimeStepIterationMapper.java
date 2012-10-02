package edu.ame.rankloud.old.simulation.timestep;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.simulation.init.NeighborWritable;
import edu.ame.rankloud.old.simulation.init.NodeDataWritable;


public class TimeStepIterationMapper extends MapReduceBase 
	implements Mapper<Text, NodeDataWritable, Text, SimMapOutputWritable> {

	public void map(Text key, 
			NodeDataWritable value,
			OutputCollector<Text, SimMapOutputWritable> output, 
			Reporter reporter) throws IOException {
		
		boolean state = value.getIsActive().get();
		if (state) {
			NeighborWritable[] neighbors = (NeighborWritable[])value.getNeighbors().toArray();
			for (NeighborWritable neighbor : neighbors) {
				output.collect(neighbor.getNodeId(), 
							   new SimMapOutputWritable(neighbor.getWeight()));
			}
			
		} // no need to send any weights if node not active
		
		// send the node data structure
		output.collect(key, new SimMapOutputWritable(value));
	}

}
