package edu.ame.rankloud.util.inputconverter;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.ame.rankloud.seedselection.DefaultFileName;
import edu.ame.rankloud.util.TextArrayWritable;

public class ALtoBin {

	public static void main(String[] args) throws IOException {
		String inDir = "adjList";
		String outDir = DefaultFileName.HDFS_INPUT_GRAPH;
		
		JobConf conf = new JobConf(ALtoBin.class);
		
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outDir), true);
		
		FileInputFormat.addInputPath(conf, new Path(inDir));
		FileOutputFormat.setOutputPath(conf, new Path(outDir));
		
		// Setting MapReduce formats and types
		conf.setInputFormat(KeyValueTextInputFormat.class);
		
		conf.setMapperClass(ALtoBinMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(TextArrayWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		long startTime = System.currentTimeMillis();
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}

}