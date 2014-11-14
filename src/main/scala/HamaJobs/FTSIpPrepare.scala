package HamaJobs

import Main.PFGAConstants._
import Parser.ParseFunctions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp._

/**
 * Created by preethu19th on 10/8/14.
 */
object FTSIpPrepare extends App {
  val hadoopConf = new Configuration()
  val conf = new HamaConfiguration(hadoopConf)
  val strArgs = new GenericOptionsParser(conf, args).getRemainingArgs
  val ip = "ip"
  //strArgs(0)
  val TMP_OUTPUT = new Path("op/op") //strArgs(1) + strArgs(1))

  conf.set("bsp.max.tasks.per.job", "72")

  val bsp = new BSPJob(conf, PiCalc.getClass)

  bsp.setJobName("Data Parser and FTS Ip Prepare")
  bsp.setBspClass(classOf[DataParseFTS])
  bsp.setInputPath(new Path(ip))
  bsp.setOutputKeyClass(classOf[T])
  bsp.setOutputValueClass(classOf[LW])
  bsp.setInputFormat(classOf[TextInputFormat])
  bsp.setOutputFormat(classOf[TextOutputFormat[T, LW]])
  FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT)
  val jobClient = new BSPJobClient(conf)
  val cluster = jobClient.getClusterStatus(true)
  bsp.setNumBspTask(cluster.getMaxTasks)
  val startTime: Long = System.nanoTime()
  bsp.waitForCompletion(true)
  System.out.println("Job Finished in " +
    (System.nanoTime - startTime) / NANOSECOND + " seconds")

  class DataParseFTS extends BSP[LW, T, T, LW, T] {
    var mapRedFunc: ParseFunctions = _
    var masterTask = ""
    var combinedOp = Map[String, Long]()

    override def setup(peer: BSPPeer[LW, T, T, LW, T]) = {
      masterTask = peer.getPeerName(peer.getPeerIndex)
      mapRedFunc = new ParseFunctions(peer.getConfiguration)
    }

    override def bsp(peer: BSPPeer[LW, T, T, LW, T]) = {
      var line = peer.readNext()
      while (line != null) {
        val (mapOpKey, mapOpVal) = mapRedFunc.mapRawLine(line.getValue)
        val newVal = combinedOp.getOrElse(mapOpKey, 0.toLong) + mapOpVal
        combinedOp = combinedOp + Tuple2(mapOpKey, newVal)
        line = peer.readNext()
      }
      for ((k, v) <- combinedOp)
        peer.send(peer.getPeerName(k.replaceAll("\\D", "").toInt % 31), new T(k + "\t" + v))

      peer.sync()


      if (peer.getPeerName.equals(masterTask)) {
        combinedOp = Map[String, Long]()
        var received = peer.getCurrentMessage()
        while (received != null) {
          val Array(year, value) = received.toString.split("\t")
          val newVal = combinedOp.getOrElse(year, 0.toLong) + value.toLong
          combinedOp = combinedOp + Tuple2(year, newVal)
          received = peer.getCurrentMessage()
        }

        for ((k, v) <- combinedOp)
          peer.send(peer.getPeerName(0), new T(k + "\t" + v))

        peer.sync()

        var (min, max, firstPass) = (0.toLong, 0.toLong, true)
        if (peer.getPeerName.equals(masterTask)) {
          combinedOp = Map[String, Long]()
          var received = peer.getCurrentMessage()
          while (received != null) {
            val Array(year, value) = received.toString.split("\t")
            val newVal = combinedOp.getOrElse(year, 0.toLong) + value.toLong
            combinedOp = combinedOp + Tuple2(year, newVal)
            if (firstPass) {
              min = newVal
              max = newVal
              firstPass = false
            } else {
              if (min > newVal) min = newVal
              if (max < newVal) max = newVal
            }
            received = peer.getCurrentMessage()
          }
        }
        for ((k, newVal) <- combinedOp) {
          peer.write(new T(k), new LW(newVal))
        }

        if (!firstPass) {
          peer.write(new T("min"), new LW(min))
          peer.write(new T("max"), new LW(max))
        }
      }
    }
  }

}