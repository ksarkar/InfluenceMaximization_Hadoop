package edu.ame.rankloud.model.lt;

import java.io.IOException;
import java.util.ArrayList;
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


public class UniformWeightLTModel implements Model {
	
	private TaskInfo initModel;
	private TaskInfo initCascade;
	private TaskInfo timeStep;
	
	/**
	 * Constructor
	 */
	public UniformWeightLTModel() {
		initModel = new InitModelInfo();
		initCascade = new InitCascadeInfo();
		timeStep = new TimeStepInfo();
	}
	
	/**
	 * @author ksarkar1
	 * Data type for storing model data related to a node.
	 * Extends NodeData with threshold as the model param.
	 */

	public static class UWLTNodeData extends NodeData {
		
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
	
	/**
	 * InitModel mapper: graph<Text, TextArrayWritable> -> <Text, NeighborWritable> 
	 */ 
	public static class InitModelMapper extends MapReduceBase 
		implements Mapper<Text, TextArrayWritable, Text, NeighborWritable> {

		@Override
		public void map(Text key, 
						TextArrayWritable value,
						OutputCollector<Text, NeighborWritable> output, 
						Reporter reporter) throws IOException {
			
			int numNeighbors = value.get().length;
			double weight = (double)1 / numNeighbors;
			
			Text[] neighbors = (Text[]) value.toArray();
			
			NeighborWritable me = new NeighborWritable(key, new DoubleWritable(weight));
			
			for (Text neighbor : neighbors) {
				output.collect(neighbor, me);
			}	
		}
	}
	
	/**
	 * InitModel reducer:  <Text, [NeighborWritable]> -> model<Text, UWLTNodeData>
	 */
	public static class InitModelReducer extends MapReduceBase
		implements Reducer<Text, NeighborWritable, Text, UWLTNodeData> {

		@Override
		public void reduce(Text key, 
						   Iterator<NeighborWritable> values,
						   OutputCollector<Text, UWLTNodeData> output, 
						   Reporter reporter) throws IOException {
			
			ArrayList<NeighborWritable> neighborList = new ArrayList<NeighborWritable>();
			DoubleWritable threshold = new DoubleWritable(0.0);
			BooleanWritable isActive = new BooleanWritable(false);
			
			while(values.hasNext()) {
				neighborList.add(new NeighborWritable(values.next()));
			}
			
			NeighborArrayWritable neighbors = new NeighborArrayWritable
													(neighborList.toArray(new NeighborWritable[0]));
			
			UWLTNodeData nodeData = new UWLTNodeData(key,
				 									 neighbors,
				 									 threshold,
				 									 isActive);

			output.collect(key, nodeData);
		}	
	}
	
	/**
	 * Information Class for running the initModel task
	 */
	private class InitModelInfo extends InitModelTaskInfo {

		@Override
		public Class<?> getMapOutputKeyClass() {
			return Text.class;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return NeighborWritable.class;
		}

		@Override
		public Class<? extends Mapper> getMapperClass() {
			return InitModelMapper.class;
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
			return UWLTNodeData.class;
		}

		@Override
		public Class<? extends Reducer> getReducerClass() {
			return InitModelReducer.class;
		}
		
	}
	
	/**
	 * InitCascadeMapper : This map reduce task sets the value of threshold for individual nodes.
	 * It consists of only the map stage.
	 * initSim<Text, UWLTNodeData> -> run/in/data<Text, UWLTNodeData>
	 */
	public static class InitCascadeMapper extends MapReduceBase
		implements Mapper<Text, UWLTNodeData, Text, UWLTNodeData> {
		
		private Random random = new Random(System.currentTimeMillis());

		@Override
		public void map(Text key, 
						UWLTNodeData value,
						OutputCollector<Text, UWLTNodeData> output, 
						Reporter reporter) throws IOException {
			
			value.setModelParam(new DoubleWritable(random.nextDouble()));
			output.collect(key, value);
		}
	}
	
	/**
	 * Information class for running InitCascade task
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
			return InitCascadeMapper.class;
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
			return UWLTNodeData.class;
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
			UWLTNodeData.class,
			DoubleWritable.class,
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
		implements Mapper<Text, UWLTNodeData, Text, TSMapOutput> {

		@Override
		public void map(Text key, 
						UWLTNodeData value,
						OutputCollector<Text, TSMapOutput> output, 
						Reporter reporter) throws IOException {
			
			boolean state = value.getIsActive().get();
			if (state) {
				NeighborWritable[] neighbors = (NeighborWritable[])value.getNeighbors().toArray();
				for (NeighborWritable neighbor : neighbors) {
					output.collect(neighbor.getNodeId(), 
								   new TSMapOutput(neighbor.getWeight()));
				}
				
			} // no need to send any weights if node not active
			
			// send the node data structure
			output.collect(key, new TSMapOutput(value));
		}
	}
	
	/**
	 * Reducer class for a time step
	 * <Text, TSMapOutput> -> run/out/data<Text, UWLTNodeData>
	 */
	public static class TimeStepReducer extends MapReduceBase
		implements Reducer<Text, TSMapOutput, Text, UWLTNodeData> {
		
		//public static enum ActiveCounter {ACTIVATED_COUNT}

		@Override
		public void reduce(Text key, 
						   Iterator<TSMapOutput> values,
						   OutputCollector<Text, UWLTNodeData> output, 
						   Reporter reporter) throws IOException {
		
			double weightSum = 0;
			UWLTNodeData nodeData = null;
			DoubleWritable w = null;

			while (values.hasNext()) {
				Writable instance = values.next().get();
				if (instance instanceof UWLTNodeData) {
					nodeData = (UWLTNodeData) instance;
				}
				else {
					w = (DoubleWritable) instance;
					weightSum = weightSum + w.get();
				}
			}
			
			double threshold = ((DoubleWritable) nodeData.getModelParam()).get();
			
			// TODO add if check for isActive.
			
			if (weightSum >= threshold) {
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
			return UWLTNodeData.class;
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
