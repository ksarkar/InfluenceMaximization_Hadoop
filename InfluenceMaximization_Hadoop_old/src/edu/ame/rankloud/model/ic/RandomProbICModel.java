package edu.ame.rankloud.model.ic;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import edu.ame.rankloud.model.InitCascadeTaskInfo;
import edu.ame.rankloud.model.InitModelTaskInfo;
import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.TaskInfo;
import edu.ame.rankloud.model.TimeStepTaskInfo;
import edu.ame.rankloud.timestep.TimeStep.ActiveCounter;
import edu.ame.rankloud.util.NeighborArrayWritable;
import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.NodeData;
import edu.ame.rankloud.util.TextArrayWritable;

public class RandomProbICModel implements Model{
	
	private TaskInfo initModel;
	private TaskInfo initCascade;
	private TaskInfo timeStep;
	
	/**
	 * Constructor
	 */
	public RandomProbICModel() {
		initModel = new InitModelInfo();
		initCascade = new InitCascadeInfo();
		timeStep = new TimeStepInfo();
	}
	
	/**
	 * @author ksarkar1
	 * Data type for storing model data related to a node.
	 * Extends NodeData with isContiguous as the model param.
	 */

	public static class RPICNodeData extends NodeData {
		
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
	
	/**
	 * InitModel mapper: graph<Text, TextArrayWritable> -> <Text, RPICNodeData> 
	 */ 
	public static class InitModelMapper extends MapReduceBase 
		implements Mapper<Text, TextArrayWritable, Text, RPICNodeData> {
		
		private Random random = new Random(System.currentTimeMillis());

		@Override
		public void map(Text key, 
						TextArrayWritable value,
						OutputCollector<Text, RPICNodeData> output, 
						Reporter reporter) throws IOException {
			
			int numNeighbors = value.get().length;
			NeighborWritable[] neighbors = new NeighborWritable[numNeighbors];
			Text[] neighborsT = (Text[]) value.toArray();
			
			for (int i = 0; i < numNeighbors; i++) {
				neighbors[i] = new NeighborWritable(neighborsT[i], 
													new DoubleWritable(
															random.nextDouble()));
			}
			
			RPICNodeData me = new RPICNodeData(key,
											   new NeighborArrayWritable(neighbors),
											   new BooleanWritable(false),
											   new BooleanWritable(false));
			
			output.collect(key, me);
		}
	}

	/**
	 * Information Class for running the initModel task
	 */
	private class InitModelInfo extends InitModelTaskInfo {

		@Override
		public Class<?> getMapOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return null;
		}

		@Override
		public Class<? extends Mapper> getMapperClass() {
			return InitModelMapper.class;
		}

		@Override
		public int getNumReducer() {
			return 0; // no reducer class
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return RPICNodeData.class;
		}

		@Override
		public Class<? extends Reducer> getReducerClass() {
			return null;
		}
		
	}
	
	/**
	 * Information class for running InitCascade task.
	 * InitCascadeMapper : For this model no cascade initiation required.
	 * So it just copies the input to the output - IdentityMapper
	 * initSim<Text, RPICNodeData> -> run/in/data<Text, RPICNodeData>
	 */
	private class InitCascadeInfo extends InitCascadeTaskInfo {

		@Override
		public Class<?> getMapOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return null;
		}

		@Override
		public Class<? extends Mapper> getMapperClass() {
			return IdentityMapper.class;
		}

		@Override
		public int getNumReducer() {
			return 0;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return RPICNodeData.class;
		}

		@Override
		public Class<? extends Reducer> getReducerClass() {
			return null;
		}
		
	}
	
	/**
	 * Map output value data type for TimeStepMapper
	 */
	
	public static class TSMapOutput extends GenericWritable{
		
		public TSMapOutput(Writable obj) {
			super.set(obj);
		}
		
		public TSMapOutput() { }
		
		private static Class[] CLASSES = {
			RPICNodeData.class,
			BooleanWritable.class,
		};
		
		protected Class[] getTypes() {
			return CLASSES;
		}

	}
	
	/**
	 * Mapper class for a time step.
	 * in/data<Text, UWLTNodeData> -> <Text, TSMapOutput>
	 */
	
	public static class TimeStepMapper extends MapReduceBase
		implements Mapper<Text, RPICNodeData, Text, TSMapOutput> {
		
		private Random random = new Random(System.currentTimeMillis());

		@Override
		public void map(Text key, 
						RPICNodeData value,
						OutputCollector<Text, TSMapOutput> output, 
						Reporter reporter) throws IOException {
			
			boolean state = ((BooleanWritable)value.getModelParam()).get();
			// check if the node is contiguous at the current time step
			if (state) {
				NeighborWritable[] neighbors = (NeighborWritable[])value.getNeighbors().toArray();
				
				double r = 0.0;
				for (NeighborWritable neighbor : neighbors) {
					r = random.nextDouble();
					if ( r <= neighbor.getWeight().get()) {
						output.collect(neighbor.getNodeId(), 
								   new TSMapOutput(new BooleanWritable(true)));		
					}
				}
				
				// the node will not be contiguous for the next time step
				value.setModelParam(new BooleanWritable(false));
			} 
			
			// send the node data structure
			output.collect(key, new TSMapOutput(value));
		}
	}
	
	/**
	 * Reducer class for a time step
	 * <Text, TSMapOutput> -> run/out/data<Text, UWLTNodeData>
	 */
	public static class TimeStepReducer extends MapReduceBase
		implements Reducer<Text, TSMapOutput, Text, RPICNodeData> {
		
		//public static enum ActiveCounter {ACTIVATED_COUNT}

		@Override
		public void reduce(Text key, 
						   Iterator<TSMapOutput> values,
						   OutputCollector<Text, RPICNodeData> output, 
						   Reporter reporter) throws IOException {
		
			boolean isContiguous = false;
			boolean temp = false;
			RPICNodeData nodeData = null;

			while (values.hasNext()) {
				Writable instance = values.next().get();
				if (instance instanceof RPICNodeData) {
					nodeData = (RPICNodeData) instance;
				}
				else {
					temp = ((BooleanWritable) instance).get();
					if (temp)
						isContiguous = true;
				}
			}
			
			if (isContiguous) {
				if (!nodeData.getIsActive().get()) {
					// increment the global counter of activated users
					reporter.incrCounter(ActiveCounter.ACTIVATED_COUNT, 1);
					nodeData.setIsActive(new BooleanWritable(true));
				}
			}
			
			output.collect(key, nodeData);
		}
	}
	
	/**
	 * Information class for running the TimeStep task
	 */
	
	private class TimeStepInfo extends TimeStepTaskInfo {

		@Override
		public Class<?> getMapOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return TSMapOutput.class;
		}

		@Override
		public Class<? extends Mapper> getMapperClass() {
			return TimeStepMapper.class;
		}

		@Override
		public int getNumReducer() {
			return 1;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return RPICNodeData.class;
		}

		@Override
		public Class<? extends Reducer> getReducerClass() {
			return TimeStepReducer.class;
		}
		
	}

	@Override
	public TaskInfo getInitCascade() {
		return this.initCascade;
	}

	@Override
	public TaskInfo getInitModel() {
		return this.initModel;
	}

	@Override
	public TaskInfo getTimeStep() {
		return this.timeStep;
	}

}
