package MapReduceJobs.GeneratePopulationMR;

import FuzzyTimeSeries.FuzzyIndividual;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;

import java.io.IOException;
//Reucer to perform GA
public class GeneratePopulationReducer extends
        Reducer<NullWritable, Text, NullWritable, Text> {
    @Override
    public void reduce(NullWritable key,
                       Iterable<Text> values, Context context) throws
            IOException, InterruptedException {
	ArrayList<FuzzyIndividual> individuals = new ArrayList<FuzzyIndividual>();
        for (Text value : values) {
            FuzzyIndividual fuzObj = new FuzzyIndividual();
            fuzObj.setChromosome(value.toString());
            indivuduals.add(fuzObj);            
            context.write(NullWritable.get(), new Text(fuzObj.toString()));
        }
    }
}
