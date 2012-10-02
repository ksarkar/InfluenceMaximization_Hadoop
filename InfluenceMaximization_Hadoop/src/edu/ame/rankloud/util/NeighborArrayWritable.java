package edu.ame.rankloud.util;

import org.apache.hadoop.io.ArrayWritable;

public class NeighborArrayWritable extends ArrayWritable {
	
	public NeighborArrayWritable() {
		super(NeighborWritable.class);
	}
	
	public NeighborArrayWritable(NeighborWritable[] values) {
		super(NeighborWritable.class, values);
	}
	
	public String toString() {
		int len = this.get().length;
		NeighborWritable[] neighbors = (NeighborWritable[]) this.toArray();
		StringBuilder str =  new StringBuilder("NWArray[size=" + len + ",{");
		
		for (int i = 0; i < len; i++) {
			str.append(neighbors[i].toString() + ","); 
		}
		
		str.append("}]");
		
		return str.toString();
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
		
		return true;
	}

}
