package Parser.InputDataParser

import MapReduceJobs.CommonMR
import Parser.ParseFunctions
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mapreduce.Reducer

class DataParserReducer extends Reducer[T, LW, T, LW] with CommonMR {
  var mapRedFunc: ParseFunctions = _

  override def setup(conT: Reducer[T, LW, T, LW]#Context) = {
    val conf = conT.getConfiguration
    mapRedFunc = new ParseFunctions(conf)
  }

  override def reduce(key: T, values: java.lang.Iterable[LW],
                      conT: Reducer[T, LW, T, LW]#Context) = {
    val redOp = mapRedFunc.reduceRawLine(values)
    conT.write(key, new LW(redOp))
    checkMinMax(redOp)
  }

  override def cleanup(conT: Reducer[T, LW, T, LW]#Context) = {
    if (!firstRedOp) {
      conT.write(new T("min"), new LW(min))
      conT.write(new T("max"), new LW(max))
    }
  }
}