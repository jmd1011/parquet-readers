package main.scala.Fauxquet.ValuesWriters.bitpacking

import main.scala.Fauxquet.Encoders.ByteBasedBitPackingEncoder
import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, Encoding}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter
import main.scala.Fauxquet.bytes.BytesInput.BytesInput

/**
  * Created by james on 1/26/17.
  */
class ByteBitPackingValuesWriter(val bound: Int) extends ValuesWriter {
  val bitWidth = 32 - Integer.numberOfLeadingZeros(bound)
  var encoder = new ByteBasedBitPackingEncoder(bitWidth)

  override def bufferedSize(): Long = encoder.bufferedSize

  override def getAllocatedSize: Long = encoder.allocatedSize

  override def encoding: Encoding = BIT_PACKED

  override def writeInt(i: Int): Unit = this.encoder.writeInt(i)

  override def reset(): Unit = {
    encoder = new ByteBasedBitPackingEncoder(bitWidth)
  }

  override def toBytes: BytesInput = encoder.toBytes
}
