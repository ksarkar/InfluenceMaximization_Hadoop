package edu.ame.rankloud.old.simulation.init;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class NodeDataWritable implements Writable{
	
	private Text nodeId;
	private NeighborArrayWritable neighbors;
	private DoubleWritable threshold;
	private BooleanWritable isActive;
	
	public NodeDataWritable(Text nodeId, 
							NeighborArrayWritable neighbors,  
							DoubleWritable threshold,
							BooleanWritable isActive){
		this.nodeId = nodeId;
		this.neighbors = neighbors;
		this.threshold = threshold;
		this.isActive = isActive;
	}
	
	public NodeDataWritable() {	}
	
	public Text getNodeId() {
		return nodeId;
	}

	public NeighborArrayWritable getNeighbors() {
		return neighbors;
	}

	public DoubleWritable getThreshold() {
		return threshold;
	}

	public BooleanWritable getIsActive() {
		return isActive;
	}

	public void setNodeId(Text nodeId) {
		this.nodeId = nodeId;
	}

	public void setNeighbors(NeighborArrayWritable neighbors) {
		this.neighbors = neighbors;
	}

	public void setThreshold(DoubleWritable threshold) {
		this.threshold = threshold;
	}

	public void setIsActive(BooleanWritable isActive) {
		this.isActive = isActive;
	}

	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		neighbors.write(out);
		threshold.write(out);
		isActive.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		nodeId = new Text();
		nodeId.readFields(in);
		
		neighbors = new NeighborArrayWritable();
		neighbors.readFields(in);
		
		threshold = new DoubleWritable();
		threshold.readFields(in);
		
		isActive = new BooleanWritable();
		isActive.readFields(in);
	}
	
}
