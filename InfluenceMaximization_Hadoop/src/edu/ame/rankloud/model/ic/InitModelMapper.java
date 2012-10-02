package edu.ame.rankloud.model.ic;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.TextArrayWritable;


/**
 * InitModel mapper: graph<Text, TextArrayWritable> -> <Text, RPICNodeData> 
 */ 
public class InitModelMapper extends MapReduceBase 
	implements Mapper<Text, TextArrayWritable, Text, RPICNodeData> {
	
	private Random random = new Random(System.currentTimeMillis());

	@Override
	public void map(Text key, 
					TextArrayWritable value,
					OutputCollector<Text, RPICNodeData> output, 
					Reporter reporter) throws IOException {
		
		int numNeighbors = value.get().length;
		NeighborWritable[] neighbors = new NeighborWritable[numNeighbors];
		Text[] neighborsT = (Text[]) value.toArray();
		
		for (int i = 0; i < numNeighbors; i++) {
			neighbors[i] = new NeighborWritable(neighborsT[i], 
												new DoubleWritable(
														random.nextDouble()));
		}
		
		RPICNodeData me = new RPICNodeData(key,
										   new NeighborArrayWritable(neighbors),
										   new BooleanWritable(false),
										   new BooleanWritable(false));
		
		output.collect(key, me);
	}
}
