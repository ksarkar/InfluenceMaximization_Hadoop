package edu.ame.rankloud.test.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.TextArrayWritable;
import edu.ame.rankloud.util.Util;

public class TestTask extends Configured
	implements Tool {
	
	public static class TTMapper extends MapReduceBase
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
			
			for (int i = 0; i < neighbors.length; i++) {
				output.collect(neighbors[i], me);
			}	
		}		
	}
	
	public static class TTReducer extends MapReduceBase
		implements Reducer<Text, NeighborWritable, Text, Text> {

		@Override
		public void reduce(Text key, 
							Iterator<NeighborWritable> values,
							OutputCollector<Text, Text> output, 
							Reporter reporter)
				throws IOException {
			
			ArrayList<NeighborWritable> neighborList = new ArrayList<NeighborWritable>();
			
			while(values.hasNext()) {
				neighborList.add(new NeighborWritable(values.next()));
			}
			
			NeighborArrayWritable neighbors = new NeighborArrayWritable
								(neighborList.toArray(new NeighborWritable[0]));
			
			Text out = new Text(neighbors.toString());
			
			output.collect(key, out);
			
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf("testJob",
											 SequenceFileInputFormat.class, 
											 TTMapper.class, 
											 Text.class, 
											 NeighborWritable.class, 
											 1, 
											 TTReducer.class, 
											 Text.class, 
											 Text.class, 
											 TextOutputFormat.class, 
											 "test/in", 
											 "test/out");
		JobClient.runJob(conf);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new TestTask(), args);
		System.exit(res);
	}

}

