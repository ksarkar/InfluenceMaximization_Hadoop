package edu.ame.rankloud.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;

/** 
 * Useful base class for node data structure of different models. 
 * to use it, you will need to create a subclass that sets the
 * modelParamClass to be of the proper type.
 *
 * For example:
 * <code>
 * public class LTNodeData extends NodeData {
 *   public LTNodeData() { 
 *     super(DoubleWritable.class); 
 *   }	
 * }
 * </code>
 */

public class NodeData implements NodeDataWritable{
	
	private Text nodeId;
	private NeighborArrayWritable neighbors;
	private Class modelParamClass;
	private Writable modelParam;
	private BooleanWritable isActive;
	
	public NodeData(Class modelParamClass) {
		super();
		this.modelParamClass = modelParamClass;
	}
	
	public NodeData(Text nodeId, NeighborArrayWritable neighbors,
			Class modelParamClass, Writable modelParam, BooleanWritable isActive) {
		super();
		this.nodeId = nodeId;
		this.neighbors = neighbors;
		this.modelParam = modelParam;
		this.modelParamClass = modelParamClass;
		this.isActive = isActive;
	}

	public Writable getModelParam() {
		return modelParam;
	}

	public Class getModelParamClass() {
		return modelParamClass;
	}

	public void setModelParam(Writable modelParam) {
		this.modelParam = modelParam;
	}

	public void setModelParamClass(Class modelParamClass) {
		this.modelParamClass = modelParamClass;
	}

	public Text getNodeId() {
		return nodeId;
	}

	public NeighborArrayWritable getNeighbors() {
		return neighbors;
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

	public void setIsActive(BooleanWritable isActive) {
		this.isActive = isActive;
	}

	public void write(DataOutput out) throws IOException {
		nodeId.write(out);
		neighbors.write(out);
		modelParam.write(out);
		isActive.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		nodeId = new Text();
		nodeId.readFields(in);
		
		neighbors = new NeighborArrayWritable();
		neighbors.readFields(in);
		
		modelParam = WritableFactories.newInstance(modelParamClass);
		modelParam.readFields(in);
		
		isActive = new BooleanWritable();
		isActive.readFields(in);
	}

	public String toString() {
		return "ND[nodeId=" + (nodeId != null? nodeId.toString() : "(null)") + 
				",neighbors=" + (neighbors != null? neighbors.toString() : "(null)") +
				",modelParam=" + (modelParam != null? modelParam.toString() : "(null)") +
				",isActive=" + (isActive != null? isActive.toString() : "(null)") + "]";
	}
	
	public boolean equals(Object o) {
		if (!(o instanceof NodeData)) {
			return false;
		}
		
		NodeData that = (NodeData)o;
		
		return (nodeId.equals(that.getNodeId()) && (neighbors.equals(that.getNeighbors())) &&
				modelParam.equals(that.getModelParam()) && isActive.equals(that.getIsActive()));
	}
	
}
