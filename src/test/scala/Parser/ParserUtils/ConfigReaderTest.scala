package Parser.ParserUtils

import org.apache.hadoop.conf.Configuration
import org.scalatest.FlatSpec

/**
 * Created by preethu on 9/22/14.
 *
 */
class ConfigReaderTest extends FlatSpec {
  "Config" should "be empty is file is empty" in {
    val conf = new Configuration()
    ConfigReader.getConf(conf, "test.properties")
    assert(conf.size() == 0)
  }

  "Config with all properties" should "be valid" in {
    val conf = new Configuration()
    ConfigReader.getConf(conf, "parse-config.properties")
    assert(conf.size() != 0)
  }
}
