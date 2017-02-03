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
}