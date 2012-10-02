package edu.ame.rankloud.util.inputconverter;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.TextArrayWritable;

public class ALtoBin extends Configured implements Tool {

	public static void main(String[] args) throws IOException {
		int res = 0;
		
		long startTime = System.currentTimeMillis();
		try {
			res = ToolRunner.run(new ALtoBin(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		String usage = new String("Usage: ALtoBin <inDir>");
		
		if (1 != args.length) {
			System.out.println(usage);
			return 1;
		}
	 	String inDir = args[0];
	 	
		
		//String inDir = "/in/adjList";
		
		
		String outDir = DefaultFileName.HDFS_INPUT_GRAPH;
		
		JobConf conf = new JobConf(getConf(), ALtoBin.class);
		
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
		
		JobClient.runJob(conf);
		
		return 0;
	}

}