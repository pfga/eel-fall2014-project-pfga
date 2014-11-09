package FuzzyTimeSeries

import Main.PFGAConstants._
import MapReduceJobs.GeneratePopulationMR.GeneratePopulationMRDriver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import sys.process._

object Trial extends App {

  val startTime = System.nanoTime()
  /*
    val ftsIpFileName = "src/main/resources/input_fts_2.txt"
    val (annualRecords, ll, ul, numOfElements) = HelperUtils.HelperFunctions
      .readReduceOp(new Configuration(), ftsIpFileName)
    println(ll, ul, numOfElements)
    val numInd = 1000
    val p = Array.ofDim[FuzzyIndividual](numInd)
    var minMse: Double = _
    (0 until numInd).foreach { i =>
      val f = new FuzzyIndividual()
      f.generateUniverse(ul, ll, numOfElements)
      f.initializeFuzzySet(annualRecords, 3)
      f.forecastValues()
      p(i) = f
      if (i == 0) minMse = f.mse
      else if (f.mse < minMse) minMse = f.mse
    }
    println(minMse)

    val p1 = Array.ofDim[FuzzyIndividual](numInd)

    (0 until numInd).foreach { i =>
      val f = new FuzzyIndividual()
      f.setUniverse(p(i).universe.mkString(" "))
      f.initializeFuzzySet(annualRecords, 3)
      f.forecastValues()
      p1(i) = f
      if (i == 0) minMse = f.mse
      else if (f.mse < minMse) minMse = f.mse
    }

    println(minMse)
  */
  "rm -rf op".!
  "rm -rf ip/*".!
  (0 to 4).foreach { i =>
    s"touch ip/$GENERATION.$i".!
  }
  GeneratePopulationMRDriver.run(
    new Configuration(), "ip", new Path("op/ip1"), "src/main/resources/input_fts_2.txt")
  GeneratePopulationMRDriver.run(
    new Configuration(), "op/ip1", new Path("op/ip2"), "src/main/resources/input_fts_2.txt")
  val stopTime = System.nanoTime()

  println((Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / 1024 / 1024)
  println((stopTime - startTime) / 1000000000)
}