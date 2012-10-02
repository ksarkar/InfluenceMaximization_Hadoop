package edu.ame.rankloud.model.lt;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NodeData;


/**
 * @author ksarkar1
 * Data type for storing model data related to a node.
 * Extends NodeData with threshold as the model param.
 */

public class UWLTNodeData extends NodeData {
	
	public UWLTNodeData () {
		super(DoubleWritable.class);
	}
	
	public UWLTNodeData(Text nodeId, 
						NeighborArrayWritable neighbors,
						DoubleWritable modelParam,
						BooleanWritable isActive) {
		
		super(nodeId, neighbors, DoubleWritable.class, modelParam, isActive);
		
	}		
}
