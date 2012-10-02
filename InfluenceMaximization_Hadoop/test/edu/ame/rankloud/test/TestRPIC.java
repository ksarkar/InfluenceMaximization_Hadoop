package edu.ame.rankloud.test;

import java.util.Iterator;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.ic.RandomProbICModel;
import edu.ame.rankloud.common.OptResult;
import edu.ame.rankloud.seedselection.SeedSelectionStrategy;
import edu.ame.rankloud.seedselection.greedy.Greedy;

import edu.ame.rankloud.simulation.Simulation;

public class TestRPIC {
	
	public static void main(String[] args) throws Exception {
		
		runCascade(args);
		//runSimulation(args);
		//runOpt(args);
	}
	
	public static void runCascade(String[] args) throws Exception {
		Model model = new RandomProbICModel();
		
		int numRuns = 1;
		Simulation simulation = new Simulation(model, numRuns);
		
		System.out.println("Running one cascade...");
		System.out.println("Number of initial seeds: " + simulation.getNumSeed());
		
		long startTime = System.currentTimeMillis();
		long[] res = simulation.singleRun(args);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		System.out.println("Number of final active users: " + res[0]);
		System.out.println("Number of time steps: " + res[1]);
	}
	
	public static void runSimulation(String[] args) throws Exception {
		Model model = new RandomProbICModel();
		
		int numRuns = 5;
		Simulation simulation = new Simulation(model, numRuns);
		
		System.out.println("Running one simulation run...");
		System.out.println("Number of initial seeds: " + simulation.getNumSeed());
		
		long startTime = System.currentTimeMillis();
		long[] results = simulation.singleRun(args);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		long average = 0;
		System.out.println("Spread");
		for (int i = 0; i < results.length; i++) {
			average = average + results[i];
			System.out.println(results[i]);
		}
		
		System.out.println("Expected spread: " + (double) average / numRuns);
		
	}
	
	public static void runOpt(String[] args) throws Exception {
		Model model = new RandomProbICModel();
		
		int numRuns = 5;		
		int numSeeds = 2;
		SeedSelectionStrategy opt = new Greedy(model, numSeeds, numRuns);
		
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
