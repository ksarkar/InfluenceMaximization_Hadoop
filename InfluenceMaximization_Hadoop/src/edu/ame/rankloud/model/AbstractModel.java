package edu.ame.rankloud.model;

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import edu.ame.rankloud.common.DefaultFileName;

public abstract class AbstractModel implements Model {
	
	public static enum ActiveCounter {ACTIVATED_COUNT};

	@Override
	public String getInputDirName() {
		return DefaultFileName.HDFS_INPUT_MODEL;
	}

	@Override
	public Class<? extends InputFormat> getInputFormatClass() {
		return SequenceFileInputFormat.class;
	}

	@Override
	public Class<? extends OutputFormat> getOutputFormatClass() {
		return SequenceFileOutputFormat.class;
	}

	@Override
	public abstract Class<?> getOutputKeyClass();

	@Override
	public abstract Class<?> getOutputValueClass();

	@Override
	public abstract void initCascade(String[] args) throws Exception;

	@Override
	public abstract void initModel(String[] args) throws Exception;

	@Override
	public abstract long runTimeStep(String[] args) throws Exception;

}
