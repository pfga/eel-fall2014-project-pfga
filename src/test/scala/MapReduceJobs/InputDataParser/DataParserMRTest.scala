package Parser.InputDataParser

import Parser.ParserUtils.ConfigReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable => LW, Text => T}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, ReduceDriver}
import org.scalatest.FlatSpec

/**
 * Created by preethu on 10/2/14.
 */
class DataParserMRTest extends FlatSpec {

  val conf: Configuration = ConfigReader.getConf("test-parse.properties")
  val mapper = new DataParserMapper
  val mapDriver = MapDriver.newMapDriver(mapper)
  mapDriver.withConfiguration(conf)

  val reducer = new DataParserReducer
  val redDriver = ReduceDriver.newReduceDriver(reducer)
  redDriver.withConfiguration(conf)

  "DataParserMapper" should "produce appropriate output" in {
    mapDriver.withInput(new LW(), new T("2010-01-01,10,something,something"))
    mapDriver.withInput(new LW(), new T("2010-01-02,20"))
    mapDriver.withOutput(new T("2010"), new LW(10))
    mapDriver.withOutput(new T("2010"), new LW(20))
    mapDriver.runTest()
  }

  "DataParserReducer" should "produce appropriate output" in {
    val values1: java.util.List[LW] = java.util.Arrays.asList(new LW(10), new LW(20), new LW(30))
    val values2: java.util.List[LW] = java.util.Arrays.asList(new LW(10))
    val values3: java.util.List[LW] = java.util.Arrays.asList(new LW(70))
    redDriver.withInput(new T("2010-01-01"), values1)
    redDriver.withInput(new T("2010-01-02"), values2)
    redDriver.withInput(new T("2010-01-03"), values3)
    redDriver.withOutput(new T("2010-01-01"), new LW(60))
    redDriver.withOutput(new T("2010-01-02"), new LW(10))
    redDriver.withOutput(new T("2010-01-03"), new LW(70))
    redDriver.withOutput(new T("min"), new LW(10))
    redDriver.withOutput(new T("max"), new LW(70))
    redDriver.runTest()
  }
}