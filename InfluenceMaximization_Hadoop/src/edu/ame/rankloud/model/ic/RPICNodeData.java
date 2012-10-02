package edu.ame.rankloud.model.ic;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NodeData;

/**
 * @author ksarkar1
 * Data type for storing model data related to a node.
 * Extends NodeData with isContiguous as the model param.
 */

public class RPICNodeData extends NodeData {
	
	public RPICNodeData () {
		super(BooleanWritable.class);
	}
	
	public RPICNodeData(Text nodeId, 
						NeighborArrayWritable neighbors,
						BooleanWritable modelParam,
						BooleanWritable isActive) {
		
		super(nodeId, neighbors, DoubleWritable.class, modelParam, isActive);
		
	}	
	
	/**
	 * overloads the setIsActive() method of the NodeDataWritable 
	 * interface so that the simulation framework (initSim() function)
	 * can set the isContiguous field also
	 */
	
	public void setIsActive(BooleanWritable isActive) {
		super.setIsActive(isActive);
		super.setModelParam(isActive);
	}
}
