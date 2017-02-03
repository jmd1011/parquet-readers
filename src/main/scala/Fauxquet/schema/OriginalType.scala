package main.scala.Fauxquet.schema

/**
  * Created by james on 1/28/17.
  */
object OriginalType extends Enumeration {
  type OriginalType = Value
  val MAP,
  LIST,
  UTF8,
  MAP_KEY_VALUE,
  ENUM,
  DECIMAL,
  DATE,
  TIME_MILLIS,
  TIME_MICROS,
  TIMESTAMP_MILLIS,
  TIMESTAMP_MICROS,
  UINT_8,
  UINT_16,
  UINT_32,
  UINT_64,
  INT_8,
  INT_16,
  INT_32,
  INT_64,
  JSON,
  BSON,
  INTERVAL = Value

  def getOriginalTypeByString(string: String): OriginalType = string match {
    case "MAP" => MAP
    case "LIST" => LIST
    case "UTF8" => UTF8
    case "MAP_KEY_VALUE" => MAP_KEY_VALUE
    case "ENUM" => ENUM
    case "DECIMAL" => DECIMAL
    case "DATE" => DATE
    case "TIME_MILLIS" => TIME_MILLIS
    case "TIME_MICROS" => TIME_MICROS
    case "TIMESTAMP_MILLIS" => TIMESTAMP_MILLIS
    case "TIMESTAMP_MICROS" => TIMESTAMP_MICROS
    case "UINT_8" => UINT_8
    case "UINT_16" => UINT_16
    case "UINT_32" => UINT_32
    case "UINT_64" => UINT_64
    case "INT_8" => INT_8
    case "INT_16" => INT_16
    case "INT_32" => INT_32
    case "INT_64" => INT_64
    case "JSON" => JSON
    case "BSON" => BSON
    case "INTERVAL" => INTERVAL
    case _ => null
  }
}