package HamaJobs

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{IOUtils, DoubleWritable => DW, NullWritable => NW, Text => T}
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp._

/**
 * Created by preethu19th on 10/8/14.
 */
object PiCalc {
  val TMP_OUTPUT = new Path("pi")

  def main(args: Array[String]) {
    val conf = new HamaConfiguration()
    conf.set("bsp.max.tasks.per.job", "50")
    val bsp = new BSPJob(conf, PiCalc.getClass)

    bsp.setJobName("Pi Estimation Example")
    bsp.setBspClass(classOf[MyEstimator])
    bsp.setInputPath(new Path("ip"))
    bsp.setOutputKeyClass(classOf[T])
    bsp.setOutputValueClass(classOf[DW])
    bsp.setOutputFormat(classOf[TextOutputFormat[T, DW]])
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT)
    val jobClient = new BSPJobClient(conf)
    val cluster = jobClient.getClusterStatus(true)
    if (args.length > 0) {
      bsp.setNumBspTask(Integer.parseInt(args(0)))
    }
    else {
      bsp.setNumBspTask(cluster.getMaxTasks)
    }
    val startTime: Long = System.currentTimeMillis
    if (bsp.waitForCompletion(true)) {
      printOutput(conf)
      System.out.println("Job Finished in " + (System.currentTimeMillis - startTime) / 1000.0 + " seconds")
    }
  }

  def printOutput(conf: Configuration) = {
    val fs = FileSystem.get(conf)
    val files = fs.listStatus(TMP_OUTPUT)
    (0 until files.length).foreach { i =>
      if (files(i).getLen() > 0) {
        val in = fs.open(files(i).getPath())
        IOUtils.copyBytes(in, System.out, conf, false)
        in.close()
      }
    }

  }

  class MyEstimator extends BSP[NW, NW, T, DW, DW] {
    final val LOG = LogFactory.getLog(classOf[PiCalc.MyEstimator])
    private final val iterations: Int = 10000
    var masterTask = ""

    override def bsp(peer: BSPPeer[NW, NW, T, DW, DW]) = {
      val in = (0 until iterations).foldLeft(0) { (in, i) =>
        val x = 2.0 * Math.random() - 1.0
        val y = 2.0 * Math.random() - 1.0
        if ((Math.sqrt(x * x + y * y) < 1.0)) {
          in + 1
        } else in
      }

      val data = 4.0 * in / iterations

      peer.send(masterTask, new DW(data))
      peer.sync()
    }

    override def setup(peer: BSPPeer[NW, NW, T, DW, DW]) = {
      masterTask = peer.getPeerName(peer.getNumPeers / 2)
    }

    override def cleanup(peer: BSPPeer[NW, NW, T, DW, DW]) = {
      if (peer.getPeerName().equals(masterTask)) {
        var pi = 0.0
        val numPeers = peer.getNumCurrentMessages()
        var received = peer.getCurrentMessage()
        while (received != null) {
          pi += received.get()
          received = peer.getCurrentMessage()
        }

        pi = pi / numPeers
        peer.write(new T(peer.getAllPeerNames.length + " _"), new DW())
        peer.write(new T("Estimated value of PI is"), new DW(pi))
      }
    }
  }

}