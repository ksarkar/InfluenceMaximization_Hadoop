package edu.ame.rankloud.seedselection.greedy;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * List abstraction for a sequence file reader
 * @author ksarkar1
 *
 */
public class NodeList {
	private JobConf conf;
	private FileSystem fs;
	private SequenceFile.Reader reader = null;
	private Writable key;
	private Writable value;
	private int openFiles;
	private Path[] paths;
	
	public NodeList(String nodeListDir) throws IOException {
		this.conf = new JobConf(NodeList.class);
		Path nodeList = new Path( nodeListDir + "/part*");
		
		this.fs = FileSystem.get(conf);
		this.paths = FileUtil.stat2Paths(fs.globStatus(nodeList));
		this.openFiles = paths.length;
		
		if (this.openFiles > 0) {
			this.reader = new SequenceFile.Reader(fs, paths[0], conf);
			this.key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			this.value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		}

	}
	
	// Recursive function for iterating over multiple files
	public boolean next(StringBuilder node) throws IOException {
		
		boolean next = false;
		
		try {
			next = reader.next(key, value);
		} catch (IOException e) {
			if (reader != null) {
				IOUtils.closeStream(reader);
				this.openFiles--;
			}
			throw e;
		}
		
		if (next) {
			String sKey = key.toString();
			node.delete(0, node.length());
			node.append(sKey);
		}
		
		else { // current file is finished
			// close the current file
			if (reader != null) {
				IOUtils.closeStream(reader);
				this.openFiles--;
			}
			// open next file; if one exists
			if (this.openFiles > 0) {
				this.reader = new SequenceFile.Reader(fs, paths[paths.length - openFiles], conf);
				next = next(node);
			}
		}
		
		return next;
	}
	
	public void close() {
		if (reader != null) {
			IOUtils.closeStream(reader);
			this.openFiles--;
		}
	}
	
}
