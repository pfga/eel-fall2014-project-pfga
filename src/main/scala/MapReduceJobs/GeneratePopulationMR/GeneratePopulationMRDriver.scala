package MapReduceJobs.GeneratePopulationMR

import Main.PFGAConstants._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MultipleOutputs, TextOutputFormat}

/**
 * Created by preethu19th on 11/5/14.
 */
object GeneratePopulationMRDriver {
  def run(conf: Configuration, basePath: String, i: Int) = {

    val job = new Job(conf)
    val ip = basePath + i
    val op = new Path(basePath + (i + 1))

    job.setMapOutputKeyClass(classOf[NW])
    job.setMapOutputValueClass(classOf[T])
    job.setMapperClass(classOf[GeneratePopulationMapper])
    job.setReducerClass(classOf[ScalaGeneratePopulationReducer])
    job.setNumReduceTasks(1)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[NW, T]])
    job.setJarByClass(GeneratePopulationMRDriver.getClass)
    FileInputFormat.setInputPaths(job, s"$ip/$GENERATION*")
    FileOutputFormat.setOutputPath(job, op)
    DistributedCache.addCacheFile(
      new java.net.URI(s"$ip/$REDUCE_PART_FILENAME"), job.getConfiguration)
    MultipleOutputs.addNamedOutput(job, GENERATION,
      classOf[TextOutputFormat[NW, T]], classOf[NW], classOf[T])
    MultipleOutputs.addNamedOutput(job, BEST_IND,
      classOf[TextOutputFormat[NW, T]], classOf[NW], classOf[T])
    job.waitForCompletion(true)
  }
}
