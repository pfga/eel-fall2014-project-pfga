package MapReduceJobs.GeneratePopulationMR;

import java.util.Comparator;

/**
 * Comparator file to compare the fitness value of the individuals.
 *
 * @author ankit
 */
public class FGIndividualComparator implements Comparator<FGIndividual> {

    @Override
    public int compare(FGIndividual i1, FGIndividual i2) {

        if (i1.getMse() < i2.getMse())
            return -1;
        else if (i1.getMse() == i2.getMse())
            return 0;
        else return 1;


    }

}
