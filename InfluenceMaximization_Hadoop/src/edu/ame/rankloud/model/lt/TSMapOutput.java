package edu.ame.rankloud.model.lt;

import org.apache.hadoop.io.DoubleWritable;
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
		UWLTNodeData.class,
		DoubleWritable.class,
	};
	
	protected Class[] getTypes() {
		return CLASSES;
	}

}
