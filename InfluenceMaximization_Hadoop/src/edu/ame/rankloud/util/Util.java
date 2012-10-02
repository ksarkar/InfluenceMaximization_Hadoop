package edu.ame.rankloud.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;


public class Util {
	
	public static JobConf getMapRedJobConf(Class<?> ToolClass,
										  Configuration Conf,
										  String jobName,
									   	  Class<? extends InputFormat> inputFormatClass,
									   	  Class<? extends Mapper> mapperClass,
									   	  Class<?> mapOutputKeyClass,
									   	  Class<?> mapOutputValueClass,
									   	  int numReducer,
									   	  Class<? extends Reducer> reducerClass,
									   	  Class<?> outputKeyClass,
									   	  Class<?> outputValueClass,
									   	  Class<? extends OutputFormat> outputFormatClass,
									   	  String inputDir,
									   	  String outputDir) throws IOException {
		
		JobConf conf = null;
		if (ToolClass == null) {
			conf = new JobConf();
		}
		else {
			conf = new JobConf(Conf, ToolClass);
		}
		
		if (jobName != null)
			conf.setJobName(jobName);

		conf.setInputFormat(inputFormatClass);
		
		conf.setMapperClass(mapperClass);
		
		if (numReducer == 0) {
			conf.setNumReduceTasks(0);
			
			conf.setOutputKeyClass(outputKeyClass);
			conf.setOutputValueClass(outputValueClass);
			
			conf.setOutputFormat(outputFormatClass);
			
		} else {
			// may set actual number of reducers
			// conf.setNumReduceTasks(numReducer);
			
			conf.setMapOutputKeyClass(mapOutputKeyClass);
			conf.setMapOutputValueClass(mapOutputValueClass);
			
			conf.setReducerClass(reducerClass);
			
			conf.setOutputKeyClass(outputKeyClass);
			conf.setOutputValueClass(outputValueClass);
			
			conf.setOutputFormat(outputFormatClass);
			
		}
		
		// delete the existing target output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputDir), true);
		

		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(conf, new Path(inputDir));
		FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		
		return conf;		
		
	}

}
