package edu.ame.rankloud.model.lt;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.TextArrayWritable;


/**
 * InitModel mapper: graph<Text, TextArrayWritable> -> <Text, NeighborWritable> 
 */ 
public class InitModelMapper extends MapReduceBase 
	implements Mapper<Text, TextArrayWritable, Text, NeighborWritable> {

	@Override
	public void map(Text key, 
					TextArrayWritable value,
					OutputCollector<Text, NeighborWritable> output, 
					Reporter reporter) throws IOException {
		
		int numNeighbors = value.get().length;
		double weight = (double)1 / numNeighbors;
		
		Text[] neighbors = (Text[]) value.toArray();
		
		NeighborWritable me = new NeighborWritable(key, new DoubleWritable(weight));
		
		for (Text neighbor : neighbors) {
			output.collect(neighbor, me);
		}	
	}
}
