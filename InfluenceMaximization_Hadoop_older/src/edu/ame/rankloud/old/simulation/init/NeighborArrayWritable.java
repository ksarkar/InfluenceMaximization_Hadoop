package edu.ame.rankloud.old.simulation.init;

import org.apache.hadoop.io.ArrayWritable;

public class NeighborArrayWritable extends ArrayWritable {
	
	public NeighborArrayWritable() {
		super(NeighborWritable.class);
	}
	
	public NeighborArrayWritable(NeighborWritable[] values) {
		super(NeighborWritable.class, values);
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof NeighborArrayWritable)) {
			return false;
		}
		
		NeighborWritable[] thisArray = (NeighborWritable[])toArray();
		NeighborWritable[] thatArray = (NeighborWritable[])(((NeighborArrayWritable)o).toArray());
		
		if (thisArray.length != thatArray.length) {
			return false;
		}
		
		for (int i = 0; i < thisArray.length; i++) {
			if (!thisArray[i].equals(thatArray[i])) {
				return false;
			}
		}
		
		return false;
	}

}
