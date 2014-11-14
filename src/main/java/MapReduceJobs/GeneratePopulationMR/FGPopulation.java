package MapReduceJobs.GeneratePopulationMR;

import java.util.Arrays;
import java.util.List;

/**
 * This class stores the population of Genetic Algorithm.
 *
 * @author ankit
 */
public class FGPopulation implements Cloneable {

    public final static double oldPopulationRatio = 0.7;
    //Cross Over related constants
    public final static double crossOverPopulationRatio = 0.1;
    public final static int crossOverIndex = 3;
    //Mutation related constants.
    public final static double mutationPopulationRatio = 0.05;
    public static int mutationPoint = 3;
    FGIndividual individuals[];
    int order;
    int noOfGenes;
    int ll;
    int ul;

    /**
     * Constructor to initialize the population.
     *
     * @param pop
     * @param order
     * @param noOfGenes
     * @param ll
     * @param ul
     */
    public FGPopulation(int pop, int order, int noOfGenes, int ll, int ul) {
        individuals = new FGIndividual[pop];
        this.order = order;
        this.noOfGenes = noOfGenes;
        this.ll = ll;
        this.ul = ul;
    }

    public FGPopulation(FGIndividual[] inds) {
        individuals = new FGIndividual[inds.length];
        int i = 0;
        for (FGIndividual fi : inds) {
            individuals[i] = (FGIndividual) fi.clone();
            i++;
        }
    }

    /**
     * This method is used to generate the initial population for GA.
     */
    public void generatePopulation() {
        for (int i = 0; i < individuals.length; i++) {
            FGIndividual fi = new FGIndividual(ll, ul, noOfGenes + 1);
            saveIndividual(i, fi);
        }
    }

    /**
     * This method computes the MSE for the Input Annual Records.
     *
     * @param annualRecords
     */
    public void computePopulation(List<AnnualRecord> annualRecords) {

        for (FGIndividual fi : individuals) {
            FuzzySet fs = new FuzzySet(fi.getGenes(), order, annualRecords);
            fi.setMse(fs.computeForcastedValue());
        }
    }

    /**
     * This method stores the individual at a particular index.
     *
     * @param index
     * @param indiv
     */
    public void saveIndividual(int index, FGIndividual indiv) {
        individuals[index] = indiv;
    }

    /**
     * This method displays the entire population.
     */
    public void displayPopulation() {
        for (FGIndividual fi : this.getIndividuals()) {
            for (int i : fi.getGenes()) {
                System.out.print(i + "\t");
            }
            System.out.println("==>" + fi.getMse());

        }
    }

    /**
     * This method sorts the entire population based on the defined comparator.
     */
    public void sortPopulation() {
        Arrays.sort(this.individuals, new FGIndividualComparator());
    }

    public FGIndividual getFittest() {
        FGIndividual d = individuals[0];
        for (int i = 1; i < individuals.length; i++) {
            if (individuals[i].getMse() < d.getMse())
                d = individuals[i];
        }
        return d;
    }

    /**
     * This is the driver method to perform the GA operations.
     */
    public void evolvePopulation() {
        generateNewIndividuals();
        performCrossOver();
        performMutation();
    }

    /**
     * This method is used in the selection process of GA.
     */
    public void generateNewIndividuals() {
        int startIndex = (int) Math.round(this.individuals.length * oldPopulationRatio);
        for (int i = startIndex + 1; i < individuals.length; i++) {
            individuals[i] = getRandomBestIndiv();
        }
    }

    /**
     * This method returns the best individual.
     *
     * @return
     */
    private FGIndividual getRandomBestIndiv() {
        int lowerL = 0;
        int upperL = individuals.length - 1;
        int index1 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
        int index2 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
        if (individuals[index1].getMse() < individuals[index2].getMse())
            return individuals[index1];
        else
            return individuals[index2];
    }

    /**
     * This method is used in the cross-over process of GA.
     */
    public void performCrossOver() {
        int lowerL = individuals.length / 2 + 1;
        int upperL = individuals.length - 1;
        int crossOverCount = (int) Math.round(this.individuals.length * crossOverPopulationRatio);
        int index1, index2, t;
        while (crossOverCount >= 1) {
            index1 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
            index2 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
            //Performing Linear Cross Over
            for (int i = crossOverIndex; i < individuals[index1].getGenes().length; i++) {
                t = individuals[index1].getGenes()[i];
                individuals[index1].getGenes()[i] = individuals[index2].getGenes()[i];
                individuals[index2].getGenes()[i] = t;
            }
            Arrays.sort(individuals[index1].getGenes());
            Arrays.sort(individuals[index2].getGenes());
            crossOverCount--;
        }
    }

    public void nullifyMSE() {

        for (FGIndividual fi : individuals) {
            fi.setMse(0.0);
        }
    }

    /**
     * This method is used in the mutation process of GA.
     */
    public void performMutation() {
        int lowerL = individuals.length / 2;
        int upperL = individuals.length - 1;
        int mutationCount = (int) Math.round(this.individuals.length * mutationPopulationRatio);
        int index1, lowerVal, upperVal;

        while (mutationCount >= 1) {
            index1 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
            lowerVal = individuals[index1].getGenes()[mutationPoint - 1];
            upperVal = individuals[index1].getGenes()[mutationPoint + 1];
            individuals[index1].getGenes()[mutationPoint] = (lowerVal + upperVal) / 2;
            mutationCount--;
        }
    }

    public Object clone() {
        FGPopulation fg = new FGPopulation(this.getIndividuals());
        return fg;
    }

    public FGIndividual[] getIndividuals() {
        return individuals;
    }
}