package MapReduceJobs.GeneratePopulationMR

import Parser.ParserUtils.ConfigKeyNames._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{NullWritable => NW, Text => T}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, MultipleOutputs, TextOutputFormat}

/**
 * Created by preethu19th on 11/5/14.
 */
object GeneratePopulationMRDriver {
  def prepare(conf: Configuration, gaOp: String, fileName: String) = {
    val ga0Path = new Path(s"${gaOp}0")
    val fs = FileSystem.get(ga0Path.toUri, conf)
    val GENERATION = conf.get(generation_filename)
    val REDUCE_PART_FILENAME = conf.get(reduce_part_filename)
    val newFileName = fileName.replace(REDUCE_PART_FILENAME, "TMP")

    fs.mkdirs(ga0Path)

    for (i <- (0 until conf.getInt(numMapperStr, 0))) {
      fs.create(new Path(s"${gaOp}0/$GENERATION.$i")).close()
    }
    fs.create(new Path(s"${gaOp}0/$REDUCE_PART_FILENAME")).close()
    fs.rename(new Path(fileName), new Path(newFileName))
    DistributedCache.addCacheFile(new java.net.URI(newFileName), conf)
  }

  def run(conf: Configuration, basePath: String, i: Int) = {

    val GENERATION = conf.get(generation_filename)
    val REDUCE_PART_FILENAME = conf.get(reduce_part_filename)
    val BEST_IND = conf.get(best_ind_filename)

    val job = new Job(conf)
    val ip = basePath + i
    val op = new Path(basePath + (i + 1))

    job.setMapOutputKeyClass(classOf[NW])
    job.setMapOutputValueClass(classOf[T])
    job.setMapperClass(classOf[GeneratePopulationMapper])
    job.setReducerClass(classOf[GeneratePopulationReducer])
    job.setNumReduceTasks(1)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[NW, T]])
    job.setJarByClass(GeneratePopulationMRDriver.getClass)
    FileInputFormat.setInputPaths(job, s"$ip/$GENERATION*")
    FileOutputFormat.setOutputPath(job, op)
    DistributedCache.addCacheFile(
      new java.net.URI(s"$ip/$REDUCE_PART_FILENAME"),
      job.getConfiguration)
    MultipleOutputs.addNamedOutput(job, GENERATION,
      classOf[TextOutputFormat[NW, T]], classOf[NW], classOf[T])
    MultipleOutputs.addNamedOutput(job, BEST_IND,
      classOf[TextOutputFormat[NW, T]], classOf[NW], classOf[T])
    job.waitForCompletion(true)
    job.getCounters
  }
}
