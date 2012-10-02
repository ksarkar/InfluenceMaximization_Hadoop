package edu.ame.rankloud.old.simulation.init;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class InitReducer extends MapReduceBase 
	implements Reducer<Text, InitMapOutWritable, Text, NodeDataWritable> {
	
	

	public void reduce(Text key, 
					   Iterator<InitMapOutWritable> values,
					   OutputCollector<Text, NodeDataWritable> output, 
					   Reporter reporter) throws IOException {
		
		ArrayList<NeighborWritable> neighborList = new ArrayList<NeighborWritable>();
		DoubleWritable threshold = new DoubleWritable(0.0);
		BooleanWritable isActive = null;
		
		while (values.hasNext()) {
			Writable obj = values.next().get();
			
			if (obj instanceof NeighborWritable) {
				neighborList.add((NeighborWritable) obj);
			}
			
			else if (obj instanceof DoubleWritable) {
				threshold = (DoubleWritable) obj;
			}
			
			else if (obj instanceof BooleanWritable) {
				isActive = (BooleanWritable) obj;
			}
		}
		
		NeighborArrayWritable neighbors = new NeighborArrayWritable(neighborList.toArray(new NeighborWritable[0]));
		
		NodeDataWritable nodeData = new NodeDataWritable(key,
													 	 neighbors,
													 	 threshold,
													 	 isActive);
		
		output.collect(key, nodeData);
		
		
/*		
		ArrayList<NeighborWritable> neighborList = new ArrayList<NeighborWritable>();
				
		while (values.hasNext()) {
			neighborList.add(values.next());
		}
		
		NeighborWritable[] n = neighborList.toArray(new NeighborWritable[0]);
				
		output.collect(key,
				       new NodeDataWritable(key,
				    		   				new NeighborArrayWritable(n),
				    		   				new DoubleWritable(random.nextDouble()),
				    		   				new BooleanWritable(seeds.contains(key.toString())? true : false)));
*/
	}

}
