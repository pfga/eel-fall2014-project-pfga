package Parser.InputDataParser

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}


object DataParserMRDriver {
  def run(conf: Configuration, ip: String, op: Path) = {
    val job = new Job(conf)
    job.setMapOutputKeyClass(classOf[T])
    job.setMapOutputValueClass(classOf[LW])
    job.setMapperClass(classOf[DataParserMapper])
    job.setReducerClass(classOf[DataParserReducer])
    job.setNumReduceTasks(19)
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setOutputFormatClass(classOf[TextOutputFormat[T, LW]])
    job.setJarByClass(DataParserMRDriver.getClass)
    FileInputFormat.setInputPaths(job, ip)
    FileOutputFormat.setOutputPath(job, op)
    job.waitForCompletion(true)
  }
}