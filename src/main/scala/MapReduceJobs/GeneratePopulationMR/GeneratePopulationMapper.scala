package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.FuzzyIndividual
import org.apache.hadoop.io.{LongWritable => LW, NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.util.Random

class GeneratePopulationMapper
  extends Mapper[LW, T, NW, T] with SortIndividual[FuzzyIndividual] {

  var lineCnt = 0
  //It stores the entire population and is used in the next generation.
  var multipleOp: MultipleOutputs[NW, T] = _
  var mutateNumber = 0

  def compare(a: FuzzyIndividual, b: FuzzyIndividual) = compareInd(a, b)

  /*
  * This initializes the set up variables for running Mapper Code.
  */
  override def setup(conT: Mapper[LW, T, NW, T]#Context) = {
    val conf = conT.getConfiguration
    per_mapper = conf.getInt("numInd", per_mapper)
    multipleOp = new MultipleOutputs[NW, T](conT)
    mutateNumber = per_mapper * conf.getInt("mutate", 5) / 100
    readCache(conf)
  }

  override def map(key: LW, value: T,
                   conT: Mapper[LW, T, NW, T]#Context) = {
    lineCnt += 1
    addToPopulation(conT, value.toString.trim)
  }

  /*
  * It checks the eligibility for crossover/mutation operation and computing the MSE.
  * */
  def addToPopulation(conT: Mapper[LW, T, NW, T]#Context,
                      chromosomeStr: String) = {
    val f = new FuzzyIndividual()
    f.setChromosome(chromosomeStr)
    val oldMse = f.mse
    //Modify MSE and perform crossover/mutation operation  if the individual's MSE is above threshold.
    //Here, threshold is dependent on the limit value in the configuration, as per now it's 90 percentile.
    // This operation is also controllled by a  coin flip(Random.nextBoolean)
    if (f.mse > goodPop.last.mse && Random.nextBoolean) {
      val goodInd = goodPop(Math.abs(Random.nextInt()) % goodPop.length)
      f.crossOverChromosome(goodInd.chromosome)
      f.initializeFuzzySet(annualRecords, order)
      f.forecastValues()
    } else if (f.mse > goodPop.last.mse && mutateNumber > 0 && Random.nextBoolean) {
      f.mutateChromosome()
      f.initializeFuzzySet(annualRecords, order)
      f.forecastValues()
      mutateNumber -= 1
    }
    //If the newly created individual is worse than the original individual then revert to the original.
    if (f.mse > oldMse) f.setChromosome(chromosomeStr)
    populateTopList(f)
    multipleOp.write(GENERATION, NW.get(), new T(f.toString()))
  }

  override def cleanup(conT: Mapper[LW, T, NW, T]#Context) = {
    if (lineCnt == 0) createPopulation(conT)
    for (f <- topList) conT.write(NW.get(), new T(f.toString()))
    multipleOp.close()
  }

  def createPopulation(conT: Mapper[LW, T, NW, T]#Context) = {
    for (i <- 0 until per_mapper) {
      val f = new FuzzyIndividual()
      f.generateChromosome(ul, ll, numOfElements)
      f.initializeFuzzySet(annualRecords, order)
      f.forecastValues()
      populateTopList(f)
      multipleOp.write(GENERATION, NW.get(), new T(f.toString()))
    }
  }
}