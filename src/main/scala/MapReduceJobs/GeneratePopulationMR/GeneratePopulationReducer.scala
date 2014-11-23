package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.FuzzyIndividual
import Main.PFGAConstants._
import org.apache.hadoop.io.{NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.collection.JavaConversions._

/**
 * This is the reducer to generate the populations
 * Created by preethu on 11/13/14.
 */
class GeneratePopulationReducer
  extends Reducer[NW, T, NW, T] with SortIndividual[FuzzyIndividual] {

  var multipleOp: MultipleOutputs[NW, T] = _
  //Setup for the generation of GA population.
  override def setup(conT: Reducer[NW, T, NW, T]#Context) = {
    val conf = conT.getConfiguration
    multipleOp = new MultipleOutputs[NW, T](conT)
    readCache(conf)
  }

  def compare(a: FuzzyIndividual, b: FuzzyIndividual) = compareInd(a, b)
  //Perform the reduce operation.
  override def reduce(k: NW, v: java.lang.Iterable[T],
                      conT: Reducer[NW, T, NW, T]#Context) = {
    for (vText <- v) {
      val f = new FuzzyIndividual()
      f.setChromosome(vText.toString)
      populateTopList(f)
    }
  }
  //This is the clean up method for the multiple outputs.
  override def cleanup(conT: Reducer[NW, T, NW, T]#Context) = {
    for (f <- topList) conT.write(NW.get(), new T(f.toString()))

    val mseOp = topList.foldLeft("") { (mseOp, f) =>
      if (mseOp.isEmpty) f.mse.toString
      else mseOp + MSE_DELIM + f.mse
    }
    multipleOp.write(BEST_IND, NW.get, new T(mseOp))
    val bestInd = topList(0)

    bestInd.initializeFuzzySet(annualRecords, order)
    bestInd.forecastValues()
    for (ar <- bestInd.annualRecords) {
      multipleOp.write(BEST_IND, NW.get, new T(ar.toString()))
    }
    val mseSplits = bestInd.mse.toString.split("\\.")
    conT.getCounter(GROUP_NAME, COUNTER_NAME_0).increment(mseSplits(0).toLong)
    conT.getCounter(GROUP_NAME, COUNTER_NAME_1).increment(mseSplits(1).toLong)

    multipleOp.close()
  }
}