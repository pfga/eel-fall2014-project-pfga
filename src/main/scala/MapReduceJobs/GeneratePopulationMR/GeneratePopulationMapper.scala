package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.{AnnualRecord, FuzzyIndividual}
import Main.PFGAConstants._
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.io.{LongWritable => LW, NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs

import scala.collection.mutable.ArrayBuffer

class GeneratePopulationMapper extends Mapper[LW, T, NW, T] {
  var annualRecords: Array[(String, Int)] = _
  var ll: Int = _
  var ul: Int = _
  var numOfElements: Int = _
  var order = 3
  var numInd = 1000
  var lineCnt = 0
  var population = ArrayBuffer[AnnualRecord]()
  var multipleOp: MultipleOutputs[NW, T] = _

  override def setup(conT: Mapper[LW, T, NW, T]#Context) = {
    val conf = conT.getConfiguration
    val cacheFiles = DistributedCache.getLocalCacheFiles(conf)
    val cacheFile = cacheFiles(0).toString
    val recordValues = HelperUtils.HelperFunctions
      .readReduceOp(conf, cacheFile)
    annualRecords = recordValues._1
    ll = recordValues._2
    ul = recordValues._3
    numOfElements = recordValues._4
    order = conf.getInt("order", 3)
    numInd = conf.getInt("numInd", 10000)
    multipleOp = new MultipleOutputs[NW, T](conT)
  }

  override def map(key: LW, value: T,
                   conT: Mapper[LW, T, NW, T]#Context) = {
    lineCnt += 1
    val f = new FuzzyIndividual()
    f.setChromosome(value.toString.trim)
    f.initializeFuzzySet(annualRecords, order)
    f.forecastValues()
    //    multipleOp.write(GENERATION, NW.get(), new T(f.toString))
    conT.write(NW.get(), new T(f.toString))
  }

  override def cleanup(conT: Mapper[LW, T, NW, T]#Context) = {
    if (lineCnt == 0) {
      (0 until numInd).foreach { i =>
        val f = new FuzzyIndividual()
        f.generateChromosome(ul, ll, numOfElements)
        f.initializeFuzzySet(annualRecords, order)
        f.forecastValues()
        //        multipleOp.write(GENERATION, NW.get(), new T(f.toString))
        conT.write(NW.get(), new T(f.toString))
      }
    }
    multipleOp.close()
  }
}