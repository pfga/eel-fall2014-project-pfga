package Parser

import HelperUtils.HelperFunctions
import Parser.ParserUtils.ConfigKeyNames._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable => LW, Text => T}

import scala.collection.JavaConversions._

/**
 * Created by preethu19th on 9/25/14.
 */
class ParseFunctions(conf: Configuration) {

  lazy val schemaArr = conf.get(schema).split(",")
  lazy val dtKeyName = conf.get(loadDate)
  lazy val reduceColName = conf.get(reduceColumn)
  lazy val reduceAct = conf.get(reduceAction)
  lazy val delimiter = conf.get(delimiterStr)

  lazy val schemaMap = HelperFunctions.schemaMap(schemaArr)
  lazy val ipDtFormat = conf.get(ipDtFormatStr)
  lazy val opDtFormat = conf.get(opDtFormatStr)

  def mapRawLine(line: T) = {
    val cols = line.toString.split(delimiter, -1)
    (dtFormatConvert(HelperFunctions.getCol(cols, dtKeyName, schemaMap)),
      HelperFunctions.getCol(cols, reduceColName, schemaMap).toLong)
  }

  def dtFormatConvert(dateStr: String) = {
    val format = new java.text.SimpleDateFormat(ipDtFormat)
    val op = new java.text.SimpleDateFormat(opDtFormat)
    val ipDt = format.parse(dateStr)
    op.format(ipDt)
  }

  def reduceRawLine(values: java.lang.Iterable[LW],
                    currRedAct: String = reduceAct) = {
    values.foldLeft((0: Long, true)) { (opCols, value) =>
      val v = value.get()
      val (redOp, firstBool) = opCols
      val newRedOp = if (firstBool) {
        v
      } else {
        currRedAct match {
          case "min" => if (redOp > v) v else redOp
          case "max" => if (redOp < v) v else redOp
          case "sum" => redOp + v
          case _ => redOp
        }
      }
      (newRedOp, false)
    }._1
  }

  def mapFTSIp(line: T) = {
    val cols = line.toString.split("\t", -1)
    (cols(0), cols(1).toLong)
  }
}