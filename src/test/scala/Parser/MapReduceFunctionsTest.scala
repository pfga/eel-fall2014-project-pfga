package Parser

import Parser.ParserUtils.ConfigKeyNames._
import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec

/**
 * Created by preethu19th on 10/5/14.
 */
class MapReduceFunctionsTest extends FlatSpec {
  val conf = new Configuration()
  conf.set(ipDtFormatStr, "MM-yyyy-dd")
  conf.set(opDtFormatStr, "yyyy")
  val a = new ParseFunctions(conf)
  "dtFormatConvert" should "convertdt from ip to op format" in {
    assert(a.dtFormatConvert("12-2010-01") == "2010")
  }
}