package main.scala.Parq

import org.apache.parquet.io.api.Converter

/**
  * Created by James on 7/7/2016.
  */
abstract class GroupConverter extends Converter {
  def isPrimitive = false
  def asGroupConverter = this
  def getConverter(i: Int): Converter
  def start(): Unit
  def end(): Unit
}
