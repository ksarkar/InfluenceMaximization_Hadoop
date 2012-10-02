package edu.ame.rankloud.model;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;

public interface Model {
	public void initModel(String[] args) throws Exception;
	public void initCascade(String[] args) throws Exception;
	public long runTimeStep(String[] args) throws Exception;
	public String getInputDirName();
	Class<? extends InputFormat> getInputFormatClass();
	public Class<?> getOutputKeyClass();
	public Class<?> getOutputValueClass();
	public Class<? extends OutputFormat> getOutputFormatClass();
}
