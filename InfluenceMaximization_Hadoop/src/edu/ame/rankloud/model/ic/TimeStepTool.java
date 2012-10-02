package edu.ame.rankloud.model.ic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.model.AbstractModel.ActiveCounter;
import edu.ame.rankloud.util.Util;

public class TimeStepTool implements Tool {
	private Configuration conf;

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf(this.getClass(), 
											this.getConf(),
											this.getClass().getName(), 
											SequenceFileInputFormat.class, 
											TimeStepMapper.class, 
											Text.class, 
											TSMapOutput.class, 
											1, 
											TimeStepReducer.class, 
											Text.class, 
											RPICNodeData.class, 
											SequenceFileOutputFormat.class, 
											DefaultFileName.HDFS_RUN_IN, 
											DefaultFileName.HDFS_RUN_OUT);
		
		Counters counters = JobClient.runJob(conf).getCounters();
		return (int)counters.getCounter(ActiveCounter.ACTIVATED_COUNT);
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
