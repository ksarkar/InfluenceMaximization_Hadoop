package edu.ame.rankloud.seedselection.lazygreedy;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.common.OptResult;
import edu.ame.rankloud.seedselection.greedy.Greedy;
import edu.ame.rankloud.seedselection.greedy.NodeList;

public class LazyGreedy extends Greedy {
	

	public LazyGreedy(Model model, int numSeeds, int numRuns,
			String localSeedSetFile, String hdfsNodeListFile) {
		super(model, numSeeds, numRuns, localSeedSetFile, hdfsNodeListFile);
	}

	public LazyGreedy(Model model, int numSeeds, int numRuns) {
		super(model, numSeeds, numRuns);
	}

	/**
	 * Data structure for the lazy greedy optimization.
	 * It stores validity information and gain.
	 * Since java priority queue is a min heap and we need a max heap,
	 * we implement the comareTo() method accordingly.
	 * 
	 * Note: this class has a natural ordering that is inconsistent with equals.
	 * 
	 * @author ksarkar1
	 *
	 */

	private static class NodeGain implements Comparable<NodeGain> {
		
		private String nodeId;
		private double nodeGain;
		private boolean isValid;
		

		public NodeGain(String nodeId, double nodeGain, boolean isValid) {
			super();
			this.nodeId = nodeId;
			this.nodeGain = nodeGain;
			this.isValid = isValid;
		}

		public String getNodeId() {
			return nodeId;
		}

		public double getNodeGain() {
			return nodeGain;
		}

		public boolean isValid() {
			return isValid;
		}

		public void setNodeId(String nodeId) {
			this.nodeId = nodeId;
		}

		public void setNodeGain(double nodeGain) {
			this.nodeGain = nodeGain;
		}

		public void setValid(boolean isValid) {
			this.isValid = isValid;
		}

		/**
		 * Greater node gain value is smaller in the ordering
		 */
		
		@Override
		public int compareTo(NodeGain o) {
			return -(Double.compare(this.getNodeGain(), o.getNodeGain()));
		}
		
	}

	@Override
	public OptResult run(String[] args) throws Exception {
		// create model from adjacency list
		super.model.initModel(args);
		super.makeNodeList(args);
		
		// create priority queue of all nodes, with marginal gain delta +inf
		PriorityQueue<NodeGain> pq = createPriorityQueueofNodeGain();
		
		Set<String> seedSet = new HashSet<String>(); //  initial empty seed set
		double Max = 0; // initial zero spread
		
		int k = 0;
		
		
		while (k < this.numSeeds) {
			
			// write the current seed set to the local seed set file
			long lastSeedPos = super.writeSeedFile(seedSet);
			
			// set all gains invalid
			Iterator<NodeGain> nl = pq.iterator();
			while (nl.hasNext()) {
				nl.next().setValid(false);
			}
			
			while(true) {
				NodeGain max = pq.poll();
				if (max.isValid() == true) {
					seedSet.add(max.getNodeId());
					k++;
					Max = Max + max.getNodeGain();
					break;
				} 
				else {
					super.appendSeedFile(lastSeedPos, max.getNodeId());
					double sigma = simulation.run(args);
					double delta = sigma - Max;
					max.setNodeGain(delta);
					max.setValid(true);
					pq.add(max);
				}
			}	
		}
		
		return new OptResult(seedSet, Max);
	}

	/**
	 * Reads the nodeList files and creates a priority queue with all
	 * the node gains set ot NaN.
	 * 
	 * @return a priority queue holding all the nodes, together with its node gain
	 * @throws IOException 
	 */
	private PriorityQueue<NodeGain> createPriorityQueueofNodeGain() throws IOException {
		
		PriorityQueue<NodeGain> pq = new PriorityQueue<NodeGain>(); 
		
		NodeList nl = new NodeList(super.hdfsNodeListFile);
		
		StringBuilder nextNode = new StringBuilder();
		while (nl.next(nextNode)) {
			String nextNodeS = nextNode.toString();
			pq.add(new NodeGain(nextNodeS, Double.NaN, false));
		}
		
		return pq;
	}
	
	

}
