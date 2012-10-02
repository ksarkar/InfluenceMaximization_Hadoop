package edu.ame.rankloud.model.lt;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.model.AbstractModel;

public class UniformWeightLTModel extends AbstractModel {
	
	@Override
	public Class<?> getOutputKeyClass() {
		return Text.class;
	}

	@Override
	public Class<?> getOutputValueClass() {
		return UWLTNodeData.class;
	}
	
	@Override
	public void initModel(String[] args) throws Exception {
		System.out.println("Initializing model : /input -> /model ...");
		ToolRunner.run(new InitModelTool(), args);
	}

	@Override
	public void initCascade(String[] args) throws Exception {
		System.out.println("Initializing cascade : /model -> /run/in ...");
		ToolRunner.run(new InitCascadeTool(), args);
	}

	@Override
	public long runTimeStep(String[] args) throws Exception {
		System.out.println("Running a time step : /run/in -> /run/out ...");
		return ToolRunner.run(new TimeStepTool(), args);
	}

}
