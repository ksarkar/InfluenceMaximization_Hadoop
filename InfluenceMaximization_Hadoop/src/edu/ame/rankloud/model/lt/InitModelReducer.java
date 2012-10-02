package edu.ame.rankloud.model.lt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NeighborWritable;

/**
 * InitModel reducer:  <Text, [NeighborWritable]> -> model<Text, UWLTNodeData>
 */
public class InitModelReducer extends MapReduceBase
	implements Reducer<Text, NeighborWritable, Text, UWLTNodeData> {

	@Override
	public void reduce(Text key, 
					   Iterator<NeighborWritable> values,
					   OutputCollector<Text, UWLTNodeData> output, 
					   Reporter reporter) throws IOException {
		
		ArrayList<NeighborWritable> neighborList = new ArrayList<NeighborWritable>();
		DoubleWritable threshold = new DoubleWritable(0.0);
		BooleanWritable isActive = new BooleanWritable(false);
		
		while(values.hasNext()) {
			neighborList.add(new NeighborWritable(values.next()));
		}
		
		NeighborArrayWritable neighbors = new NeighborArrayWritable
												(neighborList.toArray(new NeighborWritable[0]));
		
		UWLTNodeData nodeData = new UWLTNodeData(key,
			 									 neighbors,
			 									 threshold,
			 									 isActive);

		output.collect(key, nodeData);
	}	
}
