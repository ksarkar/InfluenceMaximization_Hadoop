package edu.ame.rankloud.seedselection;

import edu.ame.rankloud.common.OptResult;

public interface SeedSelectionStrategy {
	public OptResult run(String[] args) throws Exception;
	
}
