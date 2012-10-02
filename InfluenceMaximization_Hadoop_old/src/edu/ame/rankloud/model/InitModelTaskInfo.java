package edu.ame.rankloud.model;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.ame.rankloud.seedselection.DefaultFileName;

public abstract class InitModelTaskInfo 
	extends TaskInfo {

	public Class<? extends InputFormat> getInputFormatClass() {
		return SequenceFileInputFormat.class;
	}
	
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return SequenceFileOutputFormat.class;
	}
	
	public String getInputDirName() {
		return DefaultFileName.HDFS_INPUT_GRAPH;
	}
	
	public String getOutputDirName() {
		return DefaultFileName.HDFS_INPUT_MODEL;
	}
	
	public boolean isRequired() {
		return true;
	}
	
}
