package edu.ame.rankloud.model.lt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.rankloud.common.DefaultFileName;
import edu.ame.rankloud.util.NeighborWritable;
import edu.ame.rankloud.util.Util;

public class InitModelTool implements Tool {
	private Configuration conf;

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf(this.getClass(), 
											this.getConf(),
											this.getClass().getName(), 
											SequenceFileInputFormat.class, 
											InitModelMapper.class, 
											Text.class, 
											NeighborWritable.class, 
											1, 
											InitModelReducer.class, 
											Text.class, 
											UWLTNodeData.class, 
											SequenceFileOutputFormat.class, 
											DefaultFileName.HDFS_INPUT_GRAPH, 
											DefaultFileName.HDFS_INPUT_MODEL);
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
