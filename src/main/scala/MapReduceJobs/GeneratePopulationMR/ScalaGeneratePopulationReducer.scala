package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.FuzzyIndividual
import Main.PFGAConstants._
import org.apache.hadoop.io.{NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.collection.JavaConversions._

/**
 * Created by preethu on 11/13/14.
 */
class ScalaGeneratePopulationReducer
  extends Reducer[NW, T, NW, T] with SortIndividual[FuzzyIndividual] {

  var multipleOp: MultipleOutputs[NW, T] = _

  override def setup(conT: Reducer[NW, T, NW, T]#Context) = {
    val conf = conT.getConfiguration
    multipleOp = new MultipleOutputs[NW, T](conT)
    readCache(conf)
  }

  def compare(a: FuzzyIndividual, b: FuzzyIndividual) = compareInd(a, b)

  override def reduce(k: NW, v: java.lang.Iterable[T],
                      conT: Reducer[NW, T, NW, T]#Context) = {
    v.foreach { vText => checkInd(vText.toString)}
  }

  def checkInd(vStr: String) = {
    val f = new FuzzyIndividual()
    f.setChromosome(vStr)
    populateTopList(f)
  }

  override def cleanup(conT: Reducer[NW, T, NW, T]#Context) = {
    topList.foreach { f =>
      conT.write(NW.get(), new T(f.toString()))
    }
    val mseOp = topList.foldLeft("") { (mseOp, f) =>
      if (mseOp.isEmpty) f.mse.toString
      else mseOp + MSE_DELIM + f.mse
    }
    multipleOp.write(BEST_IND, NW.get, new T(mseOp))
    val bestInd = topList(0)

    bestInd.initializeFuzzySet(annualRecords, order)
    bestInd.forecastValues()
    bestInd.annualRecords.foreach { ar =>
      multipleOp.write(BEST_IND, NW.get, new T(ar.toString()))
    }
    multipleOp.write(BEST_IND, NW.get, new T("mse=" + bestInd.mse.toString()))

    multipleOp.close()
  }
}