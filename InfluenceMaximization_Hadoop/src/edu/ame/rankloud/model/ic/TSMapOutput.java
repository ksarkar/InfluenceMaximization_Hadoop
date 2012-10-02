package edu.ame.rankloud.model.ic;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

/**
 * Map output value data type for TimeStepMapper
 */

public class TSMapOutput extends GenericWritable{
	
	public TSMapOutput(Writable obj) {
		super.set(obj);
	}
	
	public TSMapOutput() { }
	
	private static Class[] CLASSES = {
		RPICNodeData.class,
		BooleanWritable.class,
	};
	
	protected Class[] getTypes() {
		return CLASSES;
	}

}
