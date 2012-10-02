package edu.ame.rankloud.old.simulation.run;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import edu.ame.rankloud.old.simulation.init.NodeDataWritable;

public class InitRun {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(
				edu.ame.rankloud.old.simulation.run.InitRun.class);

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("simulation/input/model"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/run/in/data"));
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(InitRunMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NodeDataWritable.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);

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
