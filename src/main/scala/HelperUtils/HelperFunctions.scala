package HelperUtils

import java.io.{BufferedReader, InputStreamReader}

import FuzzyTimeSeries.FuzzyIndividual
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

object HelperFunctions {
  /**
   * Convert schema into a Map which will provide index based on column name
   *
   * @param schemaArr
   * @return schemaMap: Map[String,Int]
   */
  def schemaMap(schemaArr: Array[String]) = {
    schemaArr.foldLeft((Map[String, Int](), 0)) {
      (opCols, col) =>
        val (colsMap, idx) = opCols
        (colsMap + Tuple2(col, idx), idx + 1)
    }._1
  }

  /**
   * Use the cols, colName and schemaMap
   * to find the required column value based
   * on column name.
   *
   * @param cols
   * @param colName
   * @param schemaMap
   * @return colVal:String
   */

  def getCol(cols: Array[String], colName: String,
             schemaMap: Map[String, Int]) = {
    cols(schemaMap(colName))
  }

  def readEventFile(conf: Configuration, opStr: String) = {
    var avgDist = Map[String, Int]()
    var (listLines, min, max) = (ArrayBuffer[(String, Int)](), 0, 0)

    def parseLine(line: String) = {
      val cols = line.split("\t")
      cols(0) match {
        case "max" => max = cols(1).toInt
        case "min" => min = cols(1).toInt
        case _ =>
          avgDist = avgDist + Tuple2(cols(0), cols(1).toInt)
          listLines.append((cols(0), cols(1).toInt)) // :: listLines
      }
    }

    baseReadCacheFile(conf, opStr, parseLine)

    val (retmin, retmax, intervals) = getRoundedIntervalMinMax(avgDist, min, max)
    (listLines.toArray, retmin, retmax, intervals)
  }

  /**
   * http://www.sciencedirect.com/science/article/pii/S0165011400000579
   * Using average distance from above paper to find intervals
   *
   * @param avgDist
   * @param ipMin
   * @param ipMax
   * @return (min:Int,max:Int,intervals:Int)
   */
  def getRoundedIntervalMinMax(avgDist: Map[String, Int], ipMin: Int, ipMax: Int) = {
    val dflVal = (0, 0, "")

    val (sumDiff, cntDiff, prevKey) = avgDist.keys
      .toArray.sorted.foldLeft(dflVal) { (op, key) =>
      val (sumDiff, cntDiff, prevKey) = op
      if (prevKey.isEmpty)
        (sumDiff, cntDiff, key)
      else {
        val diff = Math.abs(avgDist(key) - avgDist(prevKey))
        (sumDiff + diff, cntDiff + 1, key)
      }
    }

    val intervalRange = (sumDiff / cntDiff / 2)
    val base = getBase(intervalRange)

    val roundedIntervalRange = getRoundedToBase(intervalRange, -1, base)

    val min = getRoundedToBase(ipMin, -1)
    val max = getRoundedToBase(ipMax, 1)
    val numOfIntervals = (max - min) / roundedIntervalRange

    (min, max, numOfIntervals)
  }

  /**
   *
   * @param value
   * @param addVal
   * @return
   */
  def getRoundedToBase(value: Int, addVal: Int): Int = {
    val base = getBase(value)
    getRoundedToBase(value, addVal, base)
  }

  /**
   *
   * @param value
   * @param addVal
   * @param base
   * @return
   */
  def getRoundedToBase(value: Int, addVal: Int, base: Double): Int = {
    var rndVal = value
    while (rndVal % base != 0) rndVal = rndVal + addVal
    rndVal
  }

  /**
   * Find base using half of average
   *
   * @param avg
   * @return base:Double
   */
  def getBase(avg: Int): Double = {
    var redAvg = avg - 1
    var base = 0.1

    while (redAvg > 0) {
      redAvg = redAvg / 10
      base = base * 10
    }
    base
  }

  def baseReadCacheFile(conf: Configuration, opStr: String,
                        parseLine: (String => Unit)) = {
    try {
      val op = new Path(opStr)
      val fs = FileSystem.get(conf)
      val bufRead = new BufferedReader(new InputStreamReader(fs.open(op)))

      var line = bufRead.readLine()
      while (line != null) {
        if (!line.trim.isEmpty) parseLine(line)
        line = bufRead.readLine()
      }
      bufRead.close
    } catch {
      case e: Exception => println(e)
    }
  }

  def readPopulationFile(conf: Configuration, opStr: String) = {
    val pop = ArrayBuffer[FuzzyIndividual]()

    def parseLine(line: String) = {
      val f = new FuzzyIndividual()
      f.setChromosome(line)
      pop.append(f)
    }

    baseReadCacheFile(conf, opStr, parseLine)

    pop
  }
}