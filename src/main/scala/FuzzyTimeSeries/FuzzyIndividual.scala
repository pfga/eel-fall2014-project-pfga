package FuzzyTimeSeries

import Main.PFGAConstants._

import scala.collection.{mutable => m}
import scala.util.Random

class FuzzyIndividual {

  val discourseMap = m.Map[String, Int]()
  var chromosome: Array[Int] = _
  var annualRecords: Array[AnnualRecord] = _
  var mse: Double = 0.0

  override def toString = {
    s"${chromosome.mkString(SPACE_DELIM)}$MSE_DELIM$mse"
  }

  def generateChromosome(ul: Int, ll: Int, numOfElements: Int) = {
    val u = Array.ofDim[Int](numOfElements + 2)
    u(0) = ll
    u(numOfElements + 1) = ul
    val divisions = (ul - ll + 1) / numOfElements

    (1 to numOfElements).foreach { i =>
      val oldLl = u(i - 1) + 1
      val newLl = ll + (i * divisions)
      val random = Math.random()
      val limitDiff = newLl - oldLl + 1
      val randInterval = (random * limitDiff).toInt
      val universeElem = oldLl + randInterval

      //set an random entry as interval to universe
      u(i) = universeElem

      //set discourseMap according to the newly set interval
      setDiscourseMap(i, u(i), u(i - 1))
    }
    //set the last discourseMap
    setDiscourseMap(numOfElements + 1, u(numOfElements + 1), u(numOfElements))

    chromosome = u
  }

  /**
   * To initialize universe with already generated data
   * @param ipStr this will be the generated individual
   */

  def setChromosome(ipStr: String) = {
    val ipCols = ipStr.split(MSE_DELIM, 2)
    val intervalStr = ipCols(0)
    mse = ipCols(1).toDouble
    annualRecords = Array.empty
    discourseMap.empty
    var prevVal = 0
    chromosome = intervalStr.split(SPACE_DELIM).zipWithIndex.map { ipCols =>
      val (s, idx) = ipCols
      val currVal = s.toInt
      if (idx > 0)
        setDiscourseMap(idx, prevVal, currVal)
      prevVal = currVal
      currVal
    }
  }

  def setDiscourseMap(i: Int, u1: Int, u2: Int) = {
    discourseMap("A" + (i - 1)) = Math.ceil((u1 + u2) / 2).toInt
  }

  def initializeFuzzySet(ars: Array[(String, Int)], order: Int) {
    val arMap = m.Map[String, String]()
    val lfrgQueue = m.Queue[String]()

    annualRecords = Array.ofDim[AnnualRecord](ars.length)

    ars.zipWithIndex.foreach { opCols =>
      val ((timeSlot, events), idx) = opCols
      val rec = AnnualRecord(timeSlot, events)
      val currFuzzyStr = "A" + (ceilSearch(rec.events) + 1)
      rec.fuzzySet = currFuzzyStr

      val lfrg = lfrgQueue.mkString(",").replaceAll("#,", "")

      if (!lfrg.isEmpty) rec.flrgLH = lfrg
      if (lfrgQueue.isEmpty) (1 to order).foreach { i => lfrgQueue.enqueue("#")}

      lfrgQueue.dequeue()
      lfrgQueue.enqueue(currFuzzyStr)

      if (!lfrg.isEmpty) {
        if (arMap.contains(lfrg)) {
          val rHVal = arMap(lfrg)
          if (!rHVal.split(",").contains(currFuzzyStr)) {
            arMap(lfrg) = rHVal + "," + currFuzzyStr
          }
        } else {
          arMap(lfrg) = currFuzzyStr
        }
      }
      annualRecords(idx) = rec
    }
    annualRecords.foreach { rec => rec.flrgRH = arMap.getOrElse(rec.flrgLH, "")}
  }

  def ceilSearch(x: Int, low: Int = 0, high: Int = chromosome.length): Int = {
    val mid: Int = (low + high) / 2
    if (chromosome(mid) == x) mid
    else if (chromosome(mid) < x) {
      if (mid + 1 <= high && x <= chromosome(mid + 1)) mid
      else ceilSearch(x, mid + 1, high)
    }
    else {
      if (mid - 1 >= low && x > chromosome(mid - 1)) mid - 1
      else ceilSearch(x, low, mid - 1)
    }
  }

  def forecastValues() = {
    val (sqSums, numFc) = annualRecords.foldLeft(0.0, 0) { (mseCols, rec) =>
      val (sqSums, numFc) = mseCols
      val (sum, n) = rec.flrgRH.split(",").foldLeft(0, 0) { (cols, s) =>
        val (sum, n) = cols
        (sum + discourseMap.getOrElse(s, 0), n + 1)
      }
      if (n > 0) {
        rec.fcEvents = sum / n
      }
      if (rec.flrgRH.isEmpty)
        (sqSums, numFc)
      else
        (sqSums + Math.pow(rec.fcEvents - rec.events, 2), numFc + 1)
    }
    mse = Math.pow(sqSums / numFc, 0.5)
  }

  def crossOverChromosome(goodChromosome: Array[Int]) = {
    (0 until chromosome.length).foreach { i =>
      if (Random.nextBoolean()) chromosome(i) = goodChromosome(i)
      if (i > 0) setDiscourseMap(i, chromosome(i - 1), chromosome(i))
    }
  }

  def mutateChromosome() = {
    val len = chromosome.length
    val idx = Array.ofDim[Int](2)
    idx(0) = Math.abs(Random.nextInt() % len)
    idx(1) = Math.abs(Random.nextInt() % len)
    val sIdx = idx.sorted
    (sIdx(0) to sIdx(1)).foreach { i =>
      val lowerVal = if (i == 0) chromosome(i) else chromosome(i - 1)
      val upperVal = if (i == len - 1) chromosome(i) else chromosome(i + 1)
      if (upperVal - lowerVal > 1) chromosome(i) = (lowerVal + upperVal) / 2
      if (i > 0) setDiscourseMap(i, chromosome(i - 1), chromosome(i))
    }
  }
}