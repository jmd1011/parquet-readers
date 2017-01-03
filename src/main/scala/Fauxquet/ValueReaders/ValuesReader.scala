package main.scala.Fauxquet.ValueReaders

/**
  * Created by james on 1/3/17.
  */
abstract class ValuesReader {
  def initFromPage(valueCount: Int, page: Array[Byte], offset: Int): Unit

  def readValueDictionaryId(): Int

  def getNextOffset: Int

  def readBoolean(): Boolean
  def readBytes(): Array[Byte]
  def readFloat(): Float
  def readDouble(): Double
  def readInt(): Int
  def readLong(): Long
  def skip(): Unit
}
