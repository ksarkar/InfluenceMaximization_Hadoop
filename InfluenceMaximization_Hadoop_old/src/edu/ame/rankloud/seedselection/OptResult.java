package edu.ame.rankloud.seedselection;

import java.util.Set;

public class OptResult {
	private Set<String> seedSet;
	private double spread;
	
	
	public OptResult(Set<String> seedSet, double spread) {
		super();
		this.seedSet = seedSet;
		this.spread = spread;
	}
	
	public Set<String> getSeedSet() {
		return seedSet;
	}
	public double getSpread() {
		return spread;
	}
	public void setSeedSet(Set<String> seedSet) {
		this.seedSet = seedSet;
	}
	public void setSpread(double spread) {
		this.spread = spread;
	}
	
	
}
