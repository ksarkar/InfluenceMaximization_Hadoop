package edu.ame.rankloud.cascade;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.Util;

public class Cascade {
	Model model;

	public Cascade(Model model) {
		super();
		this.model = model;
	}
	
	public long run(long numSeed, String[] args) throws Exception {
		long[] result = this.singleRun(numSeed, args);
		return result[0];
	}
	
	public long[] singleRun(long numSeed, String[] args) throws Exception {
		long total = numSeed;
		long timeSteps = 0;
		
		model.initCascade(args);
		
		boolean run = true;
		
		while(run) {
			long count = model.runTimeStep(args);
			
			if (count > 0) {
				total = total + count;
				run = true;
			} else {
				run = false;
			}
			
			this.recycleOutput(args);
			timeSteps++;
		}
		
		long[] result = new long[2];
		result[0] = total;
		result[1] = timeSteps;
		return result;
	}

	/**
	 * delete files in run/in/ folder
	 * copy the data from the out to in directory for the next iterationy
	 * @throws Exception 
	 */
	
	private void recycleOutput(String[] args) throws Exception {
		System.out.println("Recycling data : /run/out -> /run/in ...");
		ToolRunner.run(new RecycleOutputTool(this.model), args);
	}
	
	public class RecycleOutputTool implements Tool {
		Configuration conf;
		Model model;

		public RecycleOutputTool(Model model) {
			super();
			this.model = model;
		}

		@Override
		public int run(String[] arg0) throws Exception {
			JobConf job = Util.getMapRedJobConf(this.getClass(),
												this.getConf(),
												"recycleCopy", 
												model.getInputFormatClass(), 
												IdentityMapper.class, 
												null, 
												null, 
												0, 
												null, 
												model.getOutputKeyClass(), 
												model.getOutputValueClass(), 
												model.getOutputFormatClass(), 
												DefaultFileName.HDFS_RUN_OUT, 
												DefaultFileName.HDFS_RUN_IN);

			JobClient.runJob(job);
			return 0;
		}

		@Override
		public Configuration getConf() {
			return this.conf;
		}

		@Override
		public void setConf(Configuration conf) {
			this.conf = conf;
		}
		
	}
	
	public void runDistCp(String[] args) throws Exception {
		JobConf conf = new JobConf(DistCp.class);
	    DistCp distcp = new DistCp(conf);
	    
	    ToolRunner.run(distcp, args);
	}

}
