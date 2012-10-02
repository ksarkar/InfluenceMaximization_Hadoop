package edu.ame.rankloud.old.simulation.timestep;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.ame.rankloud.old.simulation.init.NodeDataWritable;


public class TimeStepIteration {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(TimeStepIteration.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("graph_structure"));
		FileOutputFormat.setOutputPath(conf, new Path("step1"));
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(TimeStepIterationMapper.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(SimMapOutputWritable.class);
		
		conf.setReducerClass(TimeStepIterationReducer.class);
		
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
