package edu.ame.rankloud.test.misc;

import java.util.HashSet;
import java.util.PriorityQueue;


public class PQTest {

	/**
	 * Data structure for the lazy greedy optimization
	 * stores validity information and gain
	 * Since java priority queue is a min heap and we need a max heap,
	 * we implement the comareTo() method accordingly
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
	
	public static void main(String[] args) {
		HashSet<Long> s = new HashSet<Long>();
		
		s.add(4L);
		s.add(1L);
		s.add(4L);
		
		PriorityQueue<Long> pq = new PriorityQueue<Long>(s);
		
		
		while ( pq.size() != 0) {
			System.out.println(pq.poll());
		}
		
	/*	
		PriorityQueue<NodeGain> pq = new PriorityQueue<NodeGain>();
		
		pq.add(new NodeGain("one", 1.0, false));
		pq.add(new NodeGain("none", -1.0, false));
		pq.add(new NodeGain("zero", 0.0, false));
		pq.add(new NodeGain("inf", Double.POSITIVE_INFINITY, false));
		pq.add(new NodeGain("ninf", Double.NEGATIVE_INFINITY, false));
		pq.add(new NodeGain("nan", Double.NaN, false));
		
		// print in sorted order
		while ( pq.size() != 0) {
			System.out.println(pq.poll().getNodeId());
		}
		
	*/	

	}

}
