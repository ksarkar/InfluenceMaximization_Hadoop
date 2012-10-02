package edu.ame.rankloud.model;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;

public abstract class TaskInfo {
	
	public abstract Class<? extends InputFormat> getInputFormatClass();
	
	public abstract Class<? extends Mapper> getMapperClass();
	
	public abstract Class<?> getMapOutputKeyClass();
	
	public abstract Class<?> getMapOutputValueClass();
	
	public abstract int getNumReducer();
	
	public abstract Class<? extends Reducer> getReducerClass();
	
	public abstract Class<?> getOutputKeyClass();
	
	public abstract Class<?> getOutputValueClass();
	
	public abstract Class<? extends OutputFormat> getOutputFormatClass();
	
	public abstract String getInputDirName();
	
	public abstract String getOutputDirName();
	
	public abstract boolean isRequired();
	
}
