package edu.ame.rankloud.model.lt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.Util;

public class InitCascadeTool implements Tool {
	private Configuration conf;

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf(this.getClass(), 
											this.getConf(),
											this.getClass().getName(), 
											SequenceFileInputFormat.class, 
											InitCascadeMapper.class, 
											null, 
											null, 
											0, 
											null, 
											Text.class, 
											UWLTNodeData.class, 
											SequenceFileOutputFormat.class, 
											DefaultFileName.HDFS_INPUT_INIT_SIM, 
											DefaultFileName.HDFS_RUN_IN);
		JobClient.runJob(conf);
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
