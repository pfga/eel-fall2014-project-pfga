package Main

import Main.PFGAConstants._
import MapReduceJobs.GeneratePopulationMR.GeneratePopulationMRDriver
import Parser.FTSPrepareMR.FTSDataPrepareMRDriver
import Parser.InputDataParser.DataParserMRDriver
import Parser.ParserUtils.ConfigKeyNames._
import Parser.ParserUtils.ConfigReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.util.GenericOptionsParser

import scala.collection.mutable.ArrayBuffer

/**
 * Driver Program
 * Created by preethu19th on 10/5/14.
 */
object RunAlgo extends App {
  //Instantiating the timing
  val startTime = System.nanoTime()
  val conf = new Configuration()

  val strArgs = new GenericOptionsParser(conf, args).getRemainingArgs
  //Properties file to read from
  val configFileName =
    if (strArgs.size == 0) "src/main/resources/parse-config-seqday.properties"
    else strArgs(0)

  ConfigReader.getConf(conf, configFileName)
  val ip = conf.get(ip_path)
  val opBasePath = new Path(conf.get(op_base_path))
  val fs = FileSystem.get(opBasePath.toUri, conf)
  fs.delete(opBasePath, true)
  val parseData = opBasePath + conf.get(parse_data_path)
  val ftsIp = opBasePath + conf.get(fts_ip_path)
  val gaOp = opBasePath + conf.get(ga_op_path)
  DataParserMRDriver.run(conf, ip, new Path(parseData))

  //Instantiating the driver to run the data parser
  FTSDataPrepareMRDriver.run(conf, parseData, new Path(ftsIp))

  //Printing the total time involved in parsing
  println((System.nanoTime - startTime) / NANOSECOND + " seconds")

  //Setting config for the Genetic Algorithm and FTS process.
  val ftsIpFileName = s"$ftsIp/${conf.get(reduce_part_filename)}"
  GeneratePopulationMRDriver.prepare(conf, gaOp, ftsIpFileName)
  val iterationCnt = conf.getInt(num_generation, 0)
  val numOfMses = conf.getInt(numOfMsesCheck, 5)
  var checkMses = ArrayBuffer[Double]()
  var i = 0
  while (i < iterationCnt) {
    if (i - 2 > 2) fs.delete(new Path(s"$gaOp${i - 2}"), true)
    val cnt = GeneratePopulationMRDriver.run(conf, gaOp, i)
    val mse0 = cnt.getGroup(GROUP_NAME).findCounter(COUNTER_NAME_0).getValue
    val mse1 = cnt.getGroup(GROUP_NAME).findCounter(COUNTER_NAME_1).getValue
    val bestMse = s"$mse0.$mse1".toDouble
    if (checkMses.length == 0) {
      checkMses.append(bestMse)
    } else if (bestMse == checkMses(0) && checkMses.length == numOfMses) {
      i = iterationCnt
    } else if (bestMse == checkMses(0) && checkMses.length < numOfMses) {
      checkMses.append(bestMse)
    } else {
      checkMses = ArrayBuffer[Double]()
      checkMses.append(bestMse)
    }
    i += 1
  }
  val stopTime = System.nanoTime()

  //Printing the time and memory used for the prediction model.
  println((Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / 1024 / 1024)
  println((stopTime - startTime) / NANOSECOND)
}