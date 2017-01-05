package main.scala.Fauxquet.ValueReaders.rle

import main.scala.Fauxquet.{LittleEndianDecoder, SeekableArray}
import main.scala.Fauxquet.ValueReaders.ValuesReader

/**
  * Created by james on 1/3/17.
  */
class RunLengthBitPackingValuesReader(bitWidth: Int) extends ValuesReader {
  var decoder: RunLengthBitPackingHybridDecoder = _ //new RunLengthBitPackingHybridDecoder(bitWidth, arr)
  var nextOffset: Int = 0

  def initFromPage(valueCount: Int, page: SeekableArray[Byte]): Unit = {
    decoder = new RunLengthBitPackingHybridDecoder(bitWidth, new SeekableArray[Byte](page.array, page.pos))
    val length = LittleEndianDecoder.readInt(page)

    nextOffset = page.pos + length
  }

  @Deprecated
  override def initFromPage(valueCount: Int, page: Array[Byte], offset: Int): Unit = {
    initFromPage(valueCount, new SeekableArray[Byte](page, offset))
  }

  override def readValueDictionaryId(): Int = ???

  override def getNextOffset: Int = nextOffset

  override def readBoolean(): Boolean = ???

  override def readBytes(): Array[Byte] = ???

  override def readFloat(): Float = ???

  override def readDouble(): Double = ???

  override def readInt(): Int = decoder.readInt()

  override def readLong(): Long = ???

  override def skip(): Unit = ???
}