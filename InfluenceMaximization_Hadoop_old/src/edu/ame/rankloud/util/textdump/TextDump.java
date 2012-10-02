package edu.ame.rankloud.util.textdump;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.seedselection.DefaultFileName;
import edu.ame.rankloud.util.NodeData;
import edu.ame.rankloud.util.Util;


public class TextDump extends Configured 
	implements Tool {
	
	public static class TextDumpMapper<V extends NodeData> extends MapReduceBase 
		implements Mapper<Text, V, Text, Text> {

		public void map(Text key, 
						V value,
						OutputCollector<Text, Text> output, 
						Reporter reporter) throws IOException {			
			output.collect(key, new Text(value.toString()));
		}

	}

	@Override
	public int run(String[] arg0) throws Exception {
		
		String inputDir = DefaultFileName.HDFS_RUN_IN_DATA;
		String outputDir = inputDir + "/_text";
		
		JobConf conf = Util.getMapRedJobConf("TextDump", 
											 SequenceFileInputFormat.class,
											 TextDumpMapper.class,
											 null,
											 null,
											 0,
											 null, 
											 Text.class, 
											 Text.class, 
											 TextOutputFormat.class, 
											 inputDir, 
											 outputDir);
		JobClient.runJob(conf);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new TextDump(), args));
	}

}
