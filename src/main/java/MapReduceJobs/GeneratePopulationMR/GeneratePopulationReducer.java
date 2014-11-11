package MapReduceJobs.GeneratePopulationMR;

import FuzzyTimeSeries.FuzzyIndividual;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

import java.io.IOException;
import java.util.Arrays;

//Reucer to perform GA
public class GeneratePopulationReducer extends
        Reducer<NullWritable, Text, NullWritable, Text> {

    //Cross Over related constants
    public final static double crossOverPopulationRatio = 0.1;
    public final static int crossOverIndex = 3;

    FuzzyIndividual individuals[];
    int order;
    int noOfGenes;
    int ll;
    int ul;
    public final static double oldPopulationRatio = 0.7;

    //Mutation related constants.
    public final static double mutationPopulationRatio = 0.05;
    public static int mutationPoint = 3;

    @Override
    public void reduce(NullWritable key,
                       Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
	    ArrayList<FuzzyIndividual> indivs = new ArrayList<FuzzyIndividual>();
        FuzzyIndividual fuzObj;
        for (Text value : values) {
            fuzObj = new FuzzyIndividual();
            fuzObj.setChromosome(value.toString());
            indivs.add(fuzObj);
        }
        FuzzyIndividual[] individuals = (FuzzyIndividual[])indivs.toArray();
        //Call GA to Evolve Population
        evolvePopulation();
        for(int i = 0; i < individuals.length; i++){
            fuzObj = individuals[i];
            context.write(NullWritable.get(), new Text(fuzObj.toString()));
        }
       
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
    private FuzzyIndividual getRandomBestIndiv() {
        int lowerL = 0;
        int upperL = individuals.length - 1;
        int index1 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
        int index2 = lowerL + (int) (Math.random() * ((upperL - lowerL) + 1));
        if (individuals[index1].mse() < individuals[index2].mse())
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
            for (int i = crossOverIndex; i < individuals[index1].chromosome().length; i++) {
                t = individuals[index1].chromosome()[i];
                individuals[index1].chromosome()[i] = individuals[index2].chromosome()[i];
                individuals[index2].chromosome()[i] = t;
            }
            Arrays.sort(individuals[index1].chromosome());
            Arrays.sort(individuals[index2].chromosome());
            crossOverCount--;
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
            lowerVal = individuals[index1].chromosome()[mutationPoint - 1];
            upperVal = individuals[index1].chromosome()[mutationPoint + 1];
            individuals[index1].chromosome()[mutationPoint] = (lowerVal + upperVal) / 2;
            mutationCount--;
        }
    }


}
