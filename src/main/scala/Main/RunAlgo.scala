package Main


import Main.PFGAConstants._
import MapReduceJobs.GeneratePopulationMR.GeneratePopulationMRDriver
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache

import scala.sys.process._

/**
 * Created by preethu19th on 10/5/14.
 */
object RunAlgo extends App {
  val startTime = System.nanoTime()

  /*
  val conf: Configuration = ConfigReader.getConf("parse-config.properties")
  val strArgs = new GenericOptionsParser(conf, args).getRemainingArgs

  "rm -rf op".!
  val ip = "src/main/resources/410119.csv" //strArgs(0)

  val opBasePath = "op" //strArgs(1)

  val parseData = opBasePath + PARSE_DATA_PATH
  val ftsIp = opBasePath + FTS_IP_PATH
  val startTime: Long = System.nanoTime()
  DataParserMRDriver.run(conf, ip, new Path(parseData))
  FTSDataPrepareMRDriver.run(conf, parseData, new Path(ftsIp))
  System.out.println("Jobs Finished in " +
    (System.nanoTime - startTime) / NANO_SEC + " seconds")
*/
  val ftsIpFileName = "src/main/resources/input_fts_2.txt"
  //val ftsIpFileName = ftsIp + REDUCE_PART_FILENAME

  //  FGA.run(400, annualRecords.asJava, 13000, 20000, 7)
  //  FGA.run(1, annualRecords.asJava, min, max, 10)

  "rm -rf op/".!
  "mkdir -p op/ip0".!
  (0 until NUM_MAPPER).foreach { i =>
    s"touch op/ip0/$GENERATION.$i".!
  }
  s"touch op/ip0/$REDUCE_PART_FILENAME".!

  val conf = new Configuration()
  conf.setInt("per_mapper", 10000)
  conf.setInt("mapper_cnt", 5)
  DistributedCache.addCacheFile(new java.net.URI(ftsIpFileName), conf)

  (0 to 5).foreach { i =>
    GeneratePopulationMRDriver.run(conf, "op/ip", i)
  }
  val stopTime = System.nanoTime()

  println((Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / 1024 / 1024)
  println((stopTime - startTime) / 1000000000)

}