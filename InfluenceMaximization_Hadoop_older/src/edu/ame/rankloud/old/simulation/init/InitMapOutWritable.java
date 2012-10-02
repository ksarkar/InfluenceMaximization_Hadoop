package edu.ame.rankloud.old.simulation.init;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

public class InitMapOutWritable extends GenericWritable{
	
	public InitMapOutWritable(Writable obj) {
		super.set(obj);
	}
	
	public InitMapOutWritable() { }
	
	private static Class[] CLASSES = {
		NeighborWritable.class,
		DoubleWritable.class,
		BooleanWritable.class,
	};
	
	protected Class[] getTypes() {
		return CLASSES;
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof InitMapOutWritable)) {
			return false;
		}
		
		InitMapOutWritable that = (InitMapOutWritable) o;
		
		return get().equals(that.get()); 
	}

}
