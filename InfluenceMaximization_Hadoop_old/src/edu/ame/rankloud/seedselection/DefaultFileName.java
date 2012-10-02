package edu.ame.rankloud.seedselection;

public class DefaultFileName {
	public static final String HDFS_APP_ROOT = "/user/hadoop-user/sim";
	
	public static final String HDFS_INPUT = HDFS_APP_ROOT + "/input";
	public static final String HDFS_RUN = HDFS_APP_ROOT + "/run";
	public static final String HDFS_INPUT_NODELIST = HDFS_INPUT + "/nodeList";
	public static final String HDFS_INPUT_GRAPH = HDFS_INPUT + "/graph";
	public static final String HDFS_INPUT_MODEL = HDFS_INPUT + "/model";
	public static final String HDFS_INPUT_SEED_LIST = HDFS_INPUT + "/seedList/seed_list.txt";
	public static final String HDFS_INPUT_INIT_SIM = HDFS_INPUT + "/initSim";
	public static final String HDFS_RUN_IN = HDFS_RUN + "/in";
	public static final String HDFS_RUN_OUT = HDFS_RUN + "/out";
	public static final String HDFS_RUN_IN_DATA = HDFS_RUN_IN + "/data";
	public static final String HDFS_RUN_OUT_DATA = HDFS_RUN_OUT + "/data";
	
	public static final String LOCAL_SEED_SET_FILE = "./data/seed_list.txt";
}
