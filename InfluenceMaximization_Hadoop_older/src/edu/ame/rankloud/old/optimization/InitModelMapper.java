package edu.ame.rankloud.old.optimization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.util.TextArrayWritable;

public class InitModelMapper extends MapReduceBase 
	implements Mapper<Text, TextArrayWritable, Text, Text> {

	public void map(Text key, 
					TextArrayWritable value,
					OutputCollector<Text, Text> output, 
					Reporter reporter) throws IOException {
		output.collect(key, new Text());
	}

}
