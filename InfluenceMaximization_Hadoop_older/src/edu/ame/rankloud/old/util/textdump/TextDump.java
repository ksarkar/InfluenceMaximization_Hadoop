package edu.ame.rankloud.old.util.textdump;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.ame.rankloud.old.util.textdump.TextDumpMapper;

public class TextDump {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(TextDump.class);

		// specify input and output DIRECTORIES (not files)
		String in = "simulation/input/model";
		FileInputFormat.addInputPath(conf, new Path(in));
		FileOutputFormat.setOutputPath(conf, new Path(in + "/_text"));
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(TextDumpMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setOutputFormat(TextOutputFormat.class);
		
		client.setConf(conf);
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
