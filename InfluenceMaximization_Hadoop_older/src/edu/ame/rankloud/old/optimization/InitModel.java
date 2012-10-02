package edu.ame.rankloud.old.optimization;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

public class InitModel {

	public static void main(String[] args) {
		
		JobConf conf = new JobConf(InitModel.class);
		conf.setJobName("InitModel");
		
		// Setting MapReduce formats and types
		conf.setInputFormat(SequenceFileInputFormat.class);
		
		conf.setMapperClass(InitModelMapper.class);
		
		conf.setNumReduceTasks(0);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		// delete the existing target output folder
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path("/user/hadoop-user/simulation/input/nodeList"), true);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path("simulation/input/graph"));
		FileOutputFormat.setOutputPath(conf, new Path("simulation/input/nodeList"));
		
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
