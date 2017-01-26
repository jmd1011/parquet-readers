package main.scala.Fauxquet.ValuesReaders.bitpacking

import main.scala.Fauxquet.ValuesReaders.ValuesReader

/**
  * Created by james on 1/3/17.
  */
class ByteBitPackingValuesReader(bound: Int) extends ValuesReader {
  val VALUES_AT_A_TIME: Int = 8

  var nextOffset: Int = 0
  val bitWidth: Int = 32 - Integer.numberOfLeadingZeros(bound)
  val bytePacker: BytePacker = BytePacker_BE_1

  val decoded: Array[Int] = new Array[Int](VALUES_AT_A_TIME)
  var decodedPos: Int = VALUES_AT_A_TIME - 1
  var encoded: Array[Byte] = new Array[Byte](VALUES_AT_A_TIME)
  var encodedPos: Int = 0

  override def initFromPage(valueCount: Int, page: Array[Byte], offset: Int): Unit = {
    val effectiveBitLength  = valueCount * bitWidth
    val length = (effectiveBitLength + 7) / 8

    encoded = page
    encodedPos = offset
    decodedPos = VALUES_AT_A_TIME - 1
    nextOffset = offset + length
  }

  override def readValueDictionaryId(): Int = ???

  override def readBoolean(): Boolean = ???

  override def readBytes(): Array[Byte] = ???

  override def readFloat(): Float = ???

  override def readDouble(): Double = ???

  override def readInt(): Int = {
    this.decodedPos += 1

    if (decodedPos == decoded.length) {
      if (encodedPos + bitWidth > encoded.length) {
        val tempEncoded = new Array[Byte](bitWidth)

        for (i <- 0 until bitWidth) {
          tempEncoded(i) = encoded(encodedPos + i)
        }

        this.bytePacker.unpack8Values(tempEncoded, 0, decoded, 0)
      } else {
        bytePacker.unpack8Values(encoded, encodedPos, decoded, 0)
      }

      encodedPos += bitWidth
      decodedPos = 0
    }

    decoded(decodedPos)
  }

  override def readLong(): Long = ???

  override def skip(): Unit = ???

  override def getNextOffset: Int = nextOffset
}