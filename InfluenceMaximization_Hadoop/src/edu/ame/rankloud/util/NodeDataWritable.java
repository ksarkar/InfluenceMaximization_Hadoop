package edu.ame.rankloud.util;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

public interface NodeDataWritable extends Writable {
	
	public void setIsActive(BooleanWritable isActive);
}
	
