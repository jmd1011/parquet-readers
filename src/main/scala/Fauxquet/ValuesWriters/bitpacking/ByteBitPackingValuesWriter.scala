package main.scala.Fauxquet.ValuesWriters.bitpacking

import main.scala.Fauxquet.Encoders.ByteBasedBitPackingEncoder
import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, Encoding}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter

/**
  * Created by james on 1/26/17.
  */
class ByteBitPackingValuesWriter(val bound: Int) extends ValuesWriter {
  val bitWidth = 32 - Integer.numberOfLeadingZeros(bound)
  var encoder = new ByteBasedBitPackingEncoder(bitWidth)

  override def bufferedSize(): Long = ???

  //bunch of other stuff in Parquet -- do we need it?
  override def getAllocatedSize: Long = ???

  override def encoding: Encoding = BIT_PACKED

  override def writeInt(i: Int): Unit = this.encoder.writeInt(i)

  override def reset(): Unit = {
    encoder = new ByteBasedBitPackingEncoder(bitWidth)
  }
}