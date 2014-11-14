package Parser.ParserUtils

import Parser.ParserUtils.ConfigKeyNames._
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * Created by preethu on 9/22/14.
 */
object ConfigReader {

  def getConf(configName: String) = {
    val conf = new Configuration()
    val config = new PropertiesConfiguration(configName)
    validateConfig(config)

    if (config.isEmpty) {
      conf.clear()
    } else {
      val schemaArr = config.getStringArray(schema)
      val dtKeyName = config.getString(loadDate)
      val reduceColName = config.getString(reduceColumn)
      val reduceAct = config.getString(reduceAction)
      val delimiter = config.getString(delimiterStr)
      val ipDtFormat = config.getString(ipDtFormatStr)
      val opDtFormat = config.getString(opDtFormatStr)

      conf.set(schema, schemaArr.mkString(","))
      conf.set(loadDate, dtKeyName)
      conf.set(reduceColumn, reduceColName)
      conf.set(reduceAction, reduceAct)
      conf.set(delimiterStr, delimiter)
      conf.set(ipDtFormatStr, ipDtFormat)
      conf.set(opDtFormatStr, opDtFormat)
    }
    conf
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
      println(s"'${colName}' property not found!")
      config.clear()
    }
  }
}