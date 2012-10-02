package edu.ame.rankloud.model.lt;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


/**
 * InitCascadeMapper : This map reduce task sets the value of threshold for individual nodes.
 * It consists of only the map stage.
 * initSim<Text, UWLTNodeData> -> run/in/data<Text, UWLTNodeData>
 */
public class InitCascadeMapper extends MapReduceBase
	implements Mapper<Text, UWLTNodeData, Text, UWLTNodeData> {
	
	private Random random = new Random(System.currentTimeMillis());

	@Override
	public void map(Text key, 
					UWLTNodeData value,
					OutputCollector<Text, UWLTNodeData> output, 
					Reporter reporter) throws IOException {
		
		value.setModelParam(new DoubleWritable(random.nextDouble()));
		output.collect(key, value);
	}
}
