package Parser.FTSPrepareMR

import Parser.ParseFunctions
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mapreduce.Reducer

/**
 * Created by preethu19th on 10/2/14.
 */
class FTSDataPrepareReducer extends Reducer[T, LW, T, LW] {
  var mapRedFunc: ParseFunctions = _

  override def setup(conT: Reducer[T, LW, T, LW]#Context) = {
    mapRedFunc = new ParseFunctions(conT.getConfiguration)
  }

  override def reduce(key: T, values: java.lang.Iterable[LW],
                      conT: Reducer[T, LW, T, LW]#Context) = {
    val redAct = key.toString match {
      case "min" => "min"
      case "max" => "max"
      case _ => "sum"
    }
    val redOp = mapRedFunc.reduceRawLine(values, redAct)
    conT.write(key, new LW(redOp))
  }
}