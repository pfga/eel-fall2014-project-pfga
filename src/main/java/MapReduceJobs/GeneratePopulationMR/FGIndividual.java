package MapReduceJobs.GeneratePopulationMR;

import java.util.Arrays;

/**
 * This DS is an individual in the population.
 *
 * @author ankit
 */
public class FGIndividual implements Cloneable {

    private static int ul;
    private static int ll;
    private int[] genes;
    private double mse;

    public FGIndividual(int ll, int ul, int noOfGenes) {
        genes = new int[noOfGenes];
        FGIndividual.ul = ul;
        FGIndividual.ll = ll;
        generateIndividual();
    }

    public double getMse() {
        return mse;
    }

    public void setMse(double mse) {
        this.mse = mse;
    }

    // Create a random individual
    public void generateIndividual() {
        genes[0] = ll;
        genes[size() - 1] = ul;
        for (int i = 1; i < size() - 1; i++) {
            genes[i] = ll + (int) (Math.random() * ((ul - ll) + 1));

        }

        Arrays.sort(genes);

    }

    /* Public methods */
    public int size() {
        return genes.length;
    }

    public int[] getGenes() {
        return genes;
    }

    public void setGenes(int[] genes) {
        this.genes = genes;
    }

    public Object clone() {
        // shallow copy
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
            return null;
        }
    }


}
