package MapReduceJobs.GeneratePopulationMR

import FuzzyTimeSeries.FuzzyIndividual
import HelperUtils.HelperFunctions
import Main.PFGAConstants._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache

import scala.collection.mutable.ArrayBuffer

/**
 * Created by preethu on 11/13/14.
 */
trait SortIndividual[T] {
  var annualRecords: Array[(String, Int)] = _
  var ll: Int = _
  var ul: Int = _
  var numOfElements: Int = _
  var order = 3

  var goodPop = ArrayBuffer[FuzzyIndividual]()

  var num_mapper = NUM_MAPPER
  var per_mapper = PER_MAPPER
  var limit = 0
  val topList = ArrayBuffer[T]()

  def compare(a: T, b: T): Boolean

  def compareDbl(a: Double, b: Double) = a < b

  def compareInd(a: FuzzyIndividual, b: FuzzyIndividual) = a.mse < b.mse


  def getNumberByPercentage(per: Int) = num_mapper * per_mapper * per / 100

  def setLimit(per: Int) = limit = getNumberByPercentage(per)

  def populateTopList(element: T) = {
    if (topList.length == 0) topList.append(element)
    else {
      var i = 0
      while (i >= 0 && i < topList.length && i < limit) {
        if (compare(element, topList(i))) {
          topList.insert(i, element)
          i = -2
        }
        i = i + 1
      }
      if (i != -1 && topList.length < limit) topList.append(element)
      if (topList.length == limit + 1) topList.remove(limit)
    }
  }

  def readCache(conf: Configuration) = {
    order = conf.getInt("order", order)

    val cacheFiles = DistributedCache.getLocalCacheFiles(conf)
    val eventFile = cacheFiles(0).toString
    val recordValues = HelperFunctions.readEventFile(conf, eventFile)
    annualRecords = recordValues._1
    ll = recordValues._2
    ul = recordValues._3
    numOfElements = recordValues._4
    val goodPopFile = cacheFiles(1).toString
    goodPop = HelperFunctions.readPopulationFile(conf, goodPopFile)
  }
}
