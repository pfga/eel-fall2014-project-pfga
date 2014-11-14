package Parser.ParserUtils

/**
 * Created by preethu on 9/22/14.
 */
object SchemaValidator {
  def checkSchema(schema: Array[String]): Boolean = {
    if (!schema.contains("load-date")) {
      println("missing 'load-date' column in schema")
      false
    } else {
      true
    }

  }

}
