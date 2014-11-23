package Parser.FTSPrepareMR

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}

/**
 * This is the driver class for the FTS process.
 * Created by preethu19th on 10/2/14.
 */
object FTSDataPrepareMRDriver {
  def run(conf: Configuration, ip: String, op: Path) = {
    val job = new Job(conf)
    job.setMapOutputKeyClass(classOf[T])
    job.setMapOutputValueClass(classOf[LW])
    //Setting the mapper and reducers
    job.setMapperClass(classOf[FTSDataPrepareMapper])
    job.setReducerClass(classOf[FTSDataPrepareReducer])
    //Setting the number of reducers
    job.setNumReduceTasks(1)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[T, LW]])
    job.setJarByClass(FTSDataPrepareMRDriver.getClass)
    //Setting the input and output paths
    FileInputFormat.setInputPaths(job, ip)
    FileOutputFormat.setOutputPath(job, op)
    //Executing the job
    job.waitForCompletion(true)
  }
}