package GAandFTSFramework;

import java.util.List;

/**
 * This is the driver class to make the predictions.
 *
 * @author ankit
 */
public class FGA {

    /**
     * Entry Point method for the prediction algorithm.
     *
     * @param popCnt
     * @param annualRecords
     * @param lowerLimit
     * @param upperLimit
     * @param intervals
     */
    public static void run(int popCnt, List<AnnualRecord> annualRecords,
                           int lowerLimit, int upperLimit, int intervals) {
        int gen = 1;
        //Initialize starting Time
        Long startTime = System.nanoTime();
        // The argument comprises of the population, order, number of Genes
        FGPopulation fgp = new FGPopulation(popCnt, 3, intervals, lowerLimit,
                upperLimit);
        //Creating the population between thresholds
        fgp.generatePopulation();
        //Iterative process for generation starts
        while (gen <= 20) {
            //Calculate the fitnss value by Fuzzy Time Series Logic
            fgp.computePopulation(annualRecords);
            //Sort the population based on the fitness value calculated
            fgp.sortPopulation();
            System.out.println("Generation " + gen + ":" +
                    fgp.getFittest().getMse());
            //Applying GA to evolve the population
            fgp.evolvePopulation();
            gen++;
        }
        Long stopTime = System.nanoTime();
        //Calculating the difference in time
        System.out.println((stopTime - startTime) / 1000000);
    }
}