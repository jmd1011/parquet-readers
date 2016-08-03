package main.scala.Parq

/**
  * Created by James on 8/1/2016.
  */
trait ColumnReader {
  def getTotalValueCount: Long
  def consume(): Unit

  def getCurrentRepetitionLevel: Int
  def getCurrentDefinitionLevel: Int

  def writeCurrentValueToConverter(): Unit
  def skip(): Unit
  def getCurrentValueDictionaryID: Int

  def getInteger: Int
  def getBoolean: Boolean
  def getLong: Long
  def getBinary: Binary
  def getFloat: Float
  def getDouble: Double

  def getDescriptor: ColumnDescriptor
}
