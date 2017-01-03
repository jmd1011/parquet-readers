package main.scala.Fauxquet.ValueReaders.rle

import main.scala.Fauxquet.ValueReaders.ValuesReader

/**
  * Created by james on 1/3/17.
  */
class RunLengthBitPackingValuesReader extends ValuesReader {
  val decoder = new RunLengthBitPackingHybridDecoder
  var nextOffset: Int = 0

  override def initFromPage(valueCount: Int, page: Array[Byte], offset: Int): Unit = ???

  override def readValueDictionaryId(): Int = ???

  override def getNextOffset: Int = nextOffset

  override def readBoolean(): Boolean = ???

  override def readBytes(): Array[Byte] = ???

  override def readFloat(): Float = ???

  override def readDouble(): Double = ???

  override def readInt(): Int = ???

  override def readLong(): Long = ???

  override def skip(): Unit = ???
}