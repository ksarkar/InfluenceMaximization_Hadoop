package edu.ame.rankloud.seedselection.greedy;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.util.NodeDataWritable;


public class InitNodeListMapper<V extends NodeDataWritable> extends MapReduceBase 
	implements Mapper<Text, V, Text, Text> {

	public void map(Text key, 
					V value,
					OutputCollector<Text, Text> output, 
					Reporter reporter) throws IOException {
		output.collect(key, new Text());
	}

}
