package edu.ame.rankloud.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NeighborWritable implements Writable {
	
	private Text nodeId;
	private DoubleWritable weight;
	
	public NeighborWritable(Text nodeId, DoubleWritable weight) {
		this.nodeId = nodeId;
		this.weight = weight;
	}
	
	public NeighborWritable () { }
	
	public NeighborWritable(NeighborWritable n) {
		this.nodeId = new Text(n.getNodeId());
		this.weight = new DoubleWritable(n.getWeight().get());
	}

	public Text getNodeId() {
		return nodeId;
	}

	public DoubleWritable getWeight() {
		return weight;
	}

	public void setNodeId(Text nodeId) {
		this.nodeId = nodeId;
	}

	public void setWeight(DoubleWritable weight) {
		this.weight = weight;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nodeId = new Text();
		nodeId.readFields(in);
		
		weight = new DoubleWritable();
		weight.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		weight.write(out);
	}
	
	public String toString() {
		return "NW[nodeId=" + (nodeId != null ? nodeId.toString() : "(null)") +
			",weight=" + (weight != null ? weight.toString() : "(null)") + "]";
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof NeighborWritable)) {
			return false;
		}
		
		NeighborWritable that = (NeighborWritable)o;
		
		return (nodeId.equals(that.getNodeId()) && (weight.equals(that.getWeight())));
	}

}
