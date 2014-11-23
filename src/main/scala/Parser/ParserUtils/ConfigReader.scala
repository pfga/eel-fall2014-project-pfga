package Parser.ParserUtils

import Parser.ParserUtils.ConfigKeyNames._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

/**
 * Created by preethu on 9/22/14.
 */
object ConfigReader {

  def getConf(conf: Configuration, configName: String) = {
    val config = new PropertiesConfiguration(configName)
    validateConfig(config)

    if (config.isEmpty) {
      conf.clear()
      sys.error("Important configuration missing")
    } else for (key <- config.getKeys) conf.set(key, config.getString(key))
  }

  def validateConfig(config: PropertiesConfiguration) = {
    containsKey(config, schema)
    containsKey(config, loadDate)
    containsKey(config, reduceColumn)
    containsKey(config, delimiterStr)
  }

  def containsKey(config: PropertiesConfiguration, colName: String) = {
    if (config.isEmpty) {
      config
    } else if (!config.containsKey(colName)) {
      sys.error(s"'$colName' property not found!")
      config.clear()
    }
  }
}