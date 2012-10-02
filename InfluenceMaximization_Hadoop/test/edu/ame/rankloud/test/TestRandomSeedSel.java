package edu.ame.rankloud.test;

import java.util.Iterator;

import edu.ame.rankloud.common.OptResult;
import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.lt.UniformWeightLTModel;
import edu.ame.rankloud.seedselection.SeedSelectionStrategy;
import edu.ame.rankloud.seedselection.random.RandomSeedSelection;

public class TestRandomSeedSel {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		runOpt(args);
	}
	
	public static void runOpt(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		
		int numRuns = 5;		
		int numSeeds = 2;
		SeedSelectionStrategy opt = new RandomSeedSelection(model, numSeeds, numRuns);
		
		long startTime = System.currentTimeMillis();
		OptResult optR = opt.run(args);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.out.println("Seed set for maximizing contagion: ");
		Iterator<String> seeds = optR.getSeedSet().iterator();
		while (seeds.hasNext()) {
			System.out.println(seeds.next());
		}
		System.out.println("Maximum achievable spread: " + optR.getSpread());
	}

}
