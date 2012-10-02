package edu.ame.rankloud.seedselection.greedy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.util.Util;

@SuppressWarnings("deprecation")
public class MakeNodeListTool implements Tool {
	private Configuration conf;
	private Model model;
	private String hdfsNodeListFile;

	public MakeNodeListTool(Model model, String hdfsNodeListFile) {
		super();
		this.model = model;
		this.hdfsNodeListFile = hdfsNodeListFile;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = null;
		conf = Util.getMapRedJobConf(this.getClass(),
									 this.getConf(),
									 "initNodeList", 
									 model.getInputFormatClass(), 
									 InitNodeListMapper.class, 
									 null, 
									 null, 
									 0, 
									 null, 
									 Text.class, 
									 Text.class, 
									 SequenceFileOutputFormat.class, 
									 model.getInputDirName(), 
									 this.hdfsNodeListFile);
		
		Counters counters = JobClient.runJob(conf).getCounters();
		return (int)counters.findCounter("org.apache.hadoop.mapred.Task$Counter", 
									0, 
									"MAP_INPUT_RECORDS").getCounter();
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
