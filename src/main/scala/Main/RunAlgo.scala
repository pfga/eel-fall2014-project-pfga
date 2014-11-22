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
 * Driver Program
 * Created by preethu19th on 10/5/14.
 */
object RunAlgo extends App {
  //Instantiating the timing
  val startTime = System.nanoTime()
  val conf = new Configuration()

  val strArgs = new GenericOptionsParser(conf, args).getRemainingArgs
  //Properties file to read from
  val configFileName = if (strArgs.size == 0) "parse-config.properties"
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
  for (i <- 0 until iterationCnt) GeneratePopulationMRDriver.run(conf, gaOp, i)
  val stopTime = System.nanoTime()

  //Printing the time and memory used for the prediction model.
  println((Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / 1024 / 1024)
  println((stopTime - startTime) / NANOSECOND)
}