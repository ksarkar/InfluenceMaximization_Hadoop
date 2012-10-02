package edu.ame.rankloud.test;

import java.util.Iterator;

import edu.ame.rankloud.model.Model;
import edu.ame.rankloud.model.lt.UniformWeightLTModel;
import edu.ame.rankloud.seedselection.OptResult;
import edu.ame.rankloud.seedselection.SeedSelectionStrategy;
import edu.ame.rankloud.seedselection.greedy.Greedy;
import edu.ame.rankloud.seedselection.random.RandomSeedSelection;
import edu.ame.rankloud.simulation.Simulation;

public class TestUWLT {

	public static void main(String[] args) throws Exception {
		
		//runCascade();
		//runSimulation();
		runOpt();
	}
	
	public static void runCascade() throws Exception {
		Model model = new UniformWeightLTModel();
		
		int numRuns = 5;
		Simulation simulation = new Simulation(numRuns, model);
		
		System.out.println("Running one cascade...");
		System.out.println("Number of initial seeds: " + simulation.getNumSeed());
		
		long startTime = System.currentTimeMillis();
		long[] res = simulation.cascadeSingleRun();
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		System.out.println("Number of final active users: " + res[0]);
		System.out.println("Number of time steps: " + res[1]);
	}
	
	public static void runSimulation() throws Exception {
		Model model = new UniformWeightLTModel();
		
		int numRuns = 5;
		Simulation simulation = new Simulation(numRuns, model);
		
		System.out.println("Running one simulation run...");
		System.out.println("Number of initial seeds: " + simulation.getNumSeed());
		
		long startTime = System.currentTimeMillis();
		long[] results = simulation.singleRun();
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
	
	public static void runOpt() throws Exception {
		Model model = new UniformWeightLTModel();
		
		int numRuns = 5;
		Simulation simulation = new Simulation(numRuns, model);
		
		int numSeed = 2;
		SeedSelectionStrategy opt = new Greedy(numSeed, model, simulation);
		
		long startTime = System.currentTimeMillis();
		OptResult optR = opt.run();
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
