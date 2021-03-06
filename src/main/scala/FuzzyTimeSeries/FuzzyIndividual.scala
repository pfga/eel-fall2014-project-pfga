package FuzzyTimeSeries

import Main.PFGAConstants._

import scala.collection.{mutable => m}
import scala.util.Random

/*
 * This class is used to compute forecasted value using the Fuzzy Time Series Algorithm for a chromosome.
 */
class FuzzyIndividual {

  val discourseMap = m.Map[String, Int]()
  var chromosome: Array[Int] = _
  var annualRecords: Array[AnnualRecord] = _
  var mse: Double = 0.0

  override def toString = {
    s"${chromosome.mkString(SPACE_DELIM)}$MSE_DELIM$mse"
  }

  /*
  * Randomly create the individual for the first generation.
  */
  def generateChromosome(ul: Int, ll: Int, numOfElements: Int) = {
    val u = Array.ofDim[Int](numOfElements + 2)
    u(0) = ll
    u(numOfElements + 1) = ul
    //Using divisions to generate even spaced intervals
    val divisions = (ul - ll + 1) / numOfElements

    for (i <- 1 to numOfElements) {
      val oldLl = u(i - 1) + 1
      val newLl = ll + (i * divisions)
      //Generating random number
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

  def setDiscourseMap(i: Int, u1: Int, u2: Int) = {
    discourseMap("A" + (i - 1)) = Math.ceil((u1 + u2) / 2).toInt
  }

  /**
   * To initialize universe with already generated data
   * this will be the generated individual. This process
   * will be used for each generation.
   * @param ipStr
   */

  def setChromosome(ipStr: String) = {
    //Splitting the intervals and pre-generated MSE from the input string
    val ipCols = ipStr.split(MSE_DELIM, 2)
    val intervalStr = ipCols(0)
    //Setting pre-generated MSE
    mse = ipCols(1).toDouble
    //Emptying the annual record to be set from the interval string
    annualRecords = Array.empty
    discourseMap.empty
    var prevVal = 0
    //Processing the interval string to create the annual record.
    chromosome = intervalStr.split(SPACE_DELIM).zipWithIndex.map { ipCols =>
      val (s, idx) = ipCols
      val currVal = s.toInt
      if (idx > 0)
        setDiscourseMap(idx, prevVal, currVal)
      prevVal = currVal
      currVal
    }
  }

  /*
  *Creating the FuzzySet map for each interval based on the order of computation.
  */
  def initializeFuzzySet(ars: Array[(String, Int)], order: Int) {
    val arMap = m.Map[String, String]()
    val lfrgQueue = m.Queue[String]()

    annualRecords = Array.ofDim[AnnualRecord](ars.length)

    for (((timeSlot, events), idx) <- ars.zipWithIndex) {
      val rec = AnnualRecord(timeSlot, events)
      val currFuzzyStr = "A" + (ceilSearch(rec.events) + 1)
      rec.fuzzySet = currFuzzyStr

      val lfrg = lfrgQueue.mkString(",").replaceAll("#,", "")

      if (!lfrg.isEmpty) rec.flrgLH = lfrg
      if (lfrgQueue.isEmpty) for (i <- 1 to order) lfrgQueue.enqueue("#")

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
    for (rec <- annualRecords) rec.flrgRH = arMap.getOrElse(rec.flrgLH, "")

  }

  /*
  *This method performs the ceilSearch for a value x.
  */
  def ceilSearch(x: Int, low: Int = 0, high: Int = chromosome.length): Int = {
    val mid: Int = (low + high) / 2
    if (chromosome(mid) == x) mid
    /*
   * If x is greater than arr[mid], then either arr[mid + 1] is ceiling of
   * x or ceiling lies in arr[mid+1...high]
   */
    else if (chromosome(mid) < x) {
      if (mid + 1 <= high && x <= chromosome(mid + 1)) mid
      else ceilSearch(x, mid + 1, high)
    }
    /*
   * If x is smaller than arr[mid], then either arr[mid] is ceiling of x
   * or ceiling lies in arr[mid-1...high]
   */
    else {
      if (mid - 1 >= low && x > chromosome(mid - 1)) mid - 1
      else ceilSearch(x, low, mid - 1)
    }
  }

  /*
  * This is used for Calculating the MSE
  * */
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
    //Getting the standard deviation.
    mse = Math.sqrt(sqSums / numFc)
  }

  /*
  * Performs the crossover operation of the GA.
  */
  def crossOverChromosome(goodChromosome: Array[Int]) = {
    for (i <- 0 until chromosome.length) {
      if (Random.nextBoolean()) chromosome(i) = goodChromosome(i)
      if (i > 0) setDiscourseMap(i, chromosome(i - 1), chromosome(i))
    }
  }

  /*
  * Performs the mutation operation of the GA.
  */
  def mutateChromosome() = {
    val len = chromosome.length
    val idx = Array.ofDim[Int](2)
    idx(0) = Math.abs(Random.nextInt() % len)
    idx(1) = Math.abs(Random.nextInt() % len)
    val sIdx = idx.sorted
    for (i <- sIdx(0) to sIdx(1)) {
      val lowerVal = if (i == 0) chromosome(i) else chromosome(i - 1)
      val upperVal = if (i == len - 1) chromosome(i) else chromosome(i + 1)
      if (upperVal - lowerVal > 1) chromosome(i) = (lowerVal + upperVal) / 2
      if (i > 0) setDiscourseMap(i, chromosome(i - 1), chromosome(i))
    }
  }
}
