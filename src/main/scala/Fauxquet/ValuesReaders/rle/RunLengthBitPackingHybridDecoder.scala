package main.scala.Fauxquet.ValuesReaders.rle

import main.scala.Fauxquet.ValuesReaders.bitpacking.BytePacker_LE_1
import main.scala.Fauxquet.{LittleEndianDecoder, SeekableArray}

/**
  * Created by james on 1/3/17.
  */

class RunLengthBitPackingHybridDecoder(bitWidth: Int, in: SeekableArray[Byte]) {
  var mode: Mode = Mode(0, "N/A")
  var currentCount: Int = 0
  var currentValue: Int = 0
  var currentBuffer: Array[Int] = _

  def readInt() : Int = {
    if (currentCount == 0) readNext()

    currentCount -= 1

    if (currentValue > 1)
      println("sumting bad")

    mode match {
      case Mode(1, "RLE") => currentValue
      case Mode(2, "PACKED") => currentBuffer(currentBuffer.length - 1 - currentCount)
      case _ => throw new Error("Not RLE or PACKED in readInt")
    }
  }

  def readNext(): Unit = {
    def readUnsignedVarInt(in: SeekableArray[Byte]): Int = { //do I have this somewhere else?
      var value = 0
      var i = 0
      var b = 0

      b = in.next

      while ((b & 128) != 0) {
        value |= (b & 127) << i
        i += 7
      }

      value | b << i
    }

    val header = readUnsignedVarInt(this.in)
    if ((header & 1) == 0)
      mode = Mode(1, "RLE")
    else
      mode = Mode(2, "PACKED")

    mode match {
      case Mode(1, "RLE") =>
        currentCount = header >>> 1
        currentValue = LittleEndianDecoder readIntPaddedOnBitWidth(in, bitWidth)
      case Mode (2, "PACKED") =>
        val numGroups = header >>> 1
        currentCount = numGroups * 8
        currentBuffer = new Array[Int](currentCount)
        val bytes = new Array[Byte](numGroups * bitWidth)
        val bytesToRead1: Int = math.ceil(currentCount * bitWidth / 8.0).asInstanceOf[Int]
        val bytesToRead = math.min(bytesToRead1, in.getBytesRemaining)

        for (i <- 0 until bytesToRead) {
          bytes(i) = in.array(i + in.pos)
        }

        var i, j = 0

        while (i < currentCount) {
          BytePacker_LE_1.unpack8Values(bytes, j, currentBuffer, i)

          i += 8
          j += bitWidth
        }
      case _ => throw new Error("Not RLE or PACKED in readNext")
    }
  }
}

case class Mode(mode: Int, name: String)