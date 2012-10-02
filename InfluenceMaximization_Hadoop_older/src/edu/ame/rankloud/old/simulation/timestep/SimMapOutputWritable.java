package edu.ame.rankloud.old.simulation.timestep;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Writable;

import edu.ame.rankloud.old.simulation.init.NodeDataWritable;

public class SimMapOutputWritable extends GenericWritable{
	
	public SimMapOutputWritable(Writable obj) {
		super.set(obj);
	}
	
	public SimMapOutputWritable() { }
	
	private static Class[] CLASSES = {
		NodeDataWritable.class,
		DoubleWritable.class,
	};
	
	protected Class[] getTypes() {
		return CLASSES;
	}

}