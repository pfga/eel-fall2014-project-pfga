package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.FuzzyIndividual
import Main.PFGAConstants._
import org.apache.hadoop.io.{LongWritable => LW, NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.util.Random

class GeneratePopulationMapper
  extends Mapper[LW, T, NW, T] with SortIndividual[FuzzyIndividual] {

  var lineCnt = 0
  var multipleOp: MultipleOutputs[NW, T] = _
  var mutateNumber = 0

  def compare(a: FuzzyIndividual, b: FuzzyIndividual) = compareInd(a, b)

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
    addToPopulation(value.toString.trim)
  }

  def addToPopulation(chromosomeStr: String) = {
    val f = new FuzzyIndividual()
    f.setChromosome(chromosomeStr)
    val oldMse = f.mse

    if (f.mse > goodPop.last.mse && Random.nextBoolean) {
      val goodInd = goodPop(Math.abs(Random.nextInt()) % goodPop.length)
      f.crossOverChromosome(goodInd.chromosome)
      f.initializeFuzzySet(annualRecords, order)
      f.forecastValues()
      //    } else
      //    if (f.mse > goodPop.last.mse && mutateNumber > 0 && Random.nextBoolean) {
      //      f.mutateChromosome()
      //      mutateNumber -= 1
    }

    if (f.mse > oldMse) f.setChromosome(chromosomeStr)
    populateTopList(f)
    multipleOp.write(GENERATION, NW.get(), new T(f.toString()))
  }

  override def cleanup(conT: Mapper[LW, T, NW, T]#Context) = {
    if (lineCnt == 0) createPopulation
    topList.foreach { f => conT.write(NW.get(), new T(f.toString()))}
    multipleOp.close()
  }

  def createPopulation() = {
    (0 until per_mapper).foreach { i =>
      val f = new FuzzyIndividual()
      f.generateChromosome(ul, ll, numOfElements)
      f.initializeFuzzySet(annualRecords, order)
      f.forecastValues()
      populateTopList(f)
      multipleOp.write(GENERATION, NW.get(), new T(f.toString()))
    }
  }
}