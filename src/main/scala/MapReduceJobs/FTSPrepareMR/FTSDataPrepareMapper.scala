package Parser.FTSPrepareMR

import MapReduceJobs.CommonMR
import Parser.ParseFunctions
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mapreduce.Mapper

/**
 * This class acts as a mapper to prepare data for the FTS +  GA algorithm by parsing data in required format..
 * Created by preethu19th on 10/2/14.
 */

class FTSDataPrepareMapper extends Mapper[LW, T, T, LW] with CommonMR {
  var mapRedFunc: ParseFunctions = _

  //This acts as a setup function for the parser
  override def setup(conT: Mapper[LW, T, T, LW]#Context) = {
    mapRedFunc = new ParseFunctions(conT.getConfiguration)
  }

  //Processing of the parsing logic takes place in this mapper.
  override def map(key: LW, value: T,
                   conT: Mapper[LW, T, T, LW]#Context) = {
    val (mapOpKey, mapOpVal) = mapRedFunc.mapFTSIp(value)
    checkMinMax(mapOpVal)
    conT.write(new T(mapOpKey), new LW(mapOpVal))
  }

  //This method is used for changing the universe of discourse in every generation for the parser.
  override def cleanup(conT: Mapper[LW, T, T, LW]#Context) = {
    if (!firstRedOp) {
      conT.write(new T("min"), new LW(min))
      conT.write(new T("max"), new LW(max))
    }
  }
}