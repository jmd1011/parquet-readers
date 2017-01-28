package main.scala.Fauxquet.column

/**
  * Created by james on 1/28/17.
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
  //def getBinary: Binary //TODO: Probably need to make Binary :(
  def getFloat: Float
  def getDouble: Double

  def columnDescriptor: ColumnDescriptor
}
