package edu.ame.rankloud.old.util.textdump;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.simulation.init.NeighborWritable;
import edu.ame.rankloud.old.simulation.init.NodeDataWritable;


public class TextDumpMapper extends MapReduceBase 
	implements Mapper<Text, NodeDataWritable, Text, Text> {

	public void map(Text key, 
					NodeDataWritable value,
					OutputCollector<Text, Text> output, 
					Reporter reporter) throws IOException {
		
		StringBuilder nodeData = new StringBuilder();
		
		nodeData.append("\nNode Id: " + value.getNodeId().toString() + "\n");
		nodeData.append("Number of neighbors: " + value.getNeighbors().get().length + "\n");
		nodeData.append("Neighbor Ids \t \t Influence Weight\n");
		
		NeighborWritable[] neighbors = (NeighborWritable[]) value.getNeighbors().toArray();
		for (int i = 0; i < neighbors.length; i++) {
			nodeData.append(neighbors[i].getNodeId().toString() 
							+ " \t \t " + 
							neighbors[i].getWeight().get() + "\n");
		}
		
		nodeData.append("\n");
		
		nodeData.append("Threshold: " + value.getThreshold().toString() + "\n");
		
		nodeData.append("State: " + value.getIsActive().toString() + "\n\n");
		
		output.collect(key, new Text(nodeData.toString()));
	}

}
