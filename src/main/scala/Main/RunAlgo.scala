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

/**
 * Created by preethu19th on 10/5/14.
 */
object RunAlgo extends App {
  val startTime = System.nanoTime()
  val conf = new Configuration()

  val strArgs = new GenericOptionsParser(conf, args).getRemainingArgs
  val configFileName = "parse-config.properties"
  //strArgs(0)
  ConfigReader.getConf(conf, configFileName)
  val ip = conf.get(ip_path)
  val opBasePath = conf.get(op_base_path)
  val fs = FileSystem.get(conf)
  fs.delete(new Path(opBasePath), true)

  val parseData = opBasePath + conf.get(parse_data_path)
  val ftsIp = opBasePath + conf.get(fts_ip_path)
  val gaOp = opBasePath + conf.get(ga_op_path)
  DataParserMRDriver.run(conf, ip, new Path(parseData))
  FTSDataPrepareMRDriver.run(conf, parseData, new Path(ftsIp))
  println((System.nanoTime - startTime) / NANOSECOND + " seconds")

  //  val ftsIpFileName = "src/main/resources/input_fts_2.txt"
  val ftsIpFileName = ftsIp + conf.get(reduce_part_filename)

  GeneratePopulationMRDriver.prepare(conf, gaOp, ftsIpFileName)
  val iterationCnt = conf.getInt(num_generation, 0)
  for (i <- 0 to iterationCnt) GeneratePopulationMRDriver.run(conf, gaOp, i)
  val stopTime = System.nanoTime()

  println((Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / 1024 / 1024)
  println((stopTime - startTime) / NANOSECOND)
}