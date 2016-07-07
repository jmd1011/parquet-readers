package main.scala.Parq

/**
  * Created by James on 7/7/2016.
  */
abstract class GroupConverter extends Converter {
  def isPrimitive = false
  override def asGroupConverter = this
  def getConverter(i: Int): Converter
  def start(): Unit
  def end(): Unit
}
