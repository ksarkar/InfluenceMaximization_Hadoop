package edu.ame.rankloud.old.simulation.run;

public class CascadeOutput {
	private long numActive;
	private long numTimeSteps;
	public CascadeOutput(long numActive, long numTimeSteps) {
		super();
		this.numActive = numActive;
		this.numTimeSteps = numTimeSteps;
	}
	public CascadeOutput() {
		super();
		this.numActive = 0;
		this.numTimeSteps = 0;
	}
	public long getNumActive() {
		return numActive;
	}
	public long getNumTimeSteps() {
		return numTimeSteps;
	}
	public void setNumActive(long numActive) {
		this.numActive = numActive;
	}
	public void setNumTimeSteps(long numTimeSteps) {
		this.numTimeSteps = numTimeSteps;
	}
	
	
}
