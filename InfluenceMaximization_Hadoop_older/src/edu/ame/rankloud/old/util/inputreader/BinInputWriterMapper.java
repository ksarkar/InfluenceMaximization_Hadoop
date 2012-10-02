package edu.ame.rankloud.old.util.inputreader;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.ame.rankloud.old.util.TextArrayWritable;

public class BinInputWriterMapper extends MapReduceBase
	implements Mapper<Text, Text, Text, TextArrayWritable> {
	
	public void map(Text key, 
					Text value,
					OutputCollector<Text, TextArrayWritable> output, 
					Reporter reporter) throws IOException {
		String line = value.toString();
		
		String[] sNodes = line.split(",\\s");
		Text[] tNodes = new Text[sNodes.length];
		for (int i = 0; i < sNodes.length; i++){
			tNodes[i] = new Text(sNodes[i]);
		}
		
		TextArrayWritable textArray = new TextArrayWritable();
		textArray.set(tNodes);
		
		output.collect(key, textArray);
	}

}
