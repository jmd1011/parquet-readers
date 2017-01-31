package main.scala.Fauxquet.Encoders

import main.scala.Fauxquet.ValuesReaders.bitpacking.BytePacker_LE_1
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput, BytesInputManager}

/**
  * Created by james on 1/26/17.
  */
class ByteBasedBitPackingEncoder(bitWidth: Int) {
  val VALUES_WRITTEN_AT_A_TIME = 8

  val input = new Array[Int](VALUES_WRITTEN_AT_A_TIME)
  var inputSize = 0

  var slabs = List[BytesInput]()
  val slabSize = bitWidth * 64 * 1024

  val packed = new Array[Byte](slabSize)
  var packedPosition = 0
  var totalValues = 0

  def initPacked(): Unit = {
    for (i <- packed.indices) {
      packed(i) = 0
    }

    packedPosition = 0
  }

  def writeInt(value: Int): Unit = {
    input(inputSize) = value
    inputSize += 1

    if (inputSize == VALUES_WRITTEN_AT_A_TIME) {
      //pack()

      if (packedPosition == slabSize) {
        slabs ::= BytesInputManager.from(packed)
        initPacked()
      }
    }
  }

  def pack(): Unit = {
    BytePacker_LE_1.pack8Values(input, 0, packed, packedPosition)
    packedPosition += bitWidth
    totalValues += inputSize
    inputSize = 0

  }

  def toBytes: BytesInput = {
    val packedByteLength = packedPosition + ((inputSize * bitWidth) + 7) / 8

    if (inputSize > 0) {
      for (i <- inputSize until input.length) {
        input(i) = 0
      }

      pack()
    }

    BytesInputManager.concat(BytesInputManager.concat(slabs), BytesInputManager.from(packed, 0, packedByteLength))
  }

  def bufferedSize: Long = ((totalValues * bitWidth) + 7) / 8
  def allocatedSize: Long = (slabs.length * slabSize) + packed.length + input.length * 4
}
