package main.scala.Fauxquet.ValuesReaders.rle

import main.scala.Fauxquet.Encoders.RunLengthBitPackingHybridEncoder
import main.scala.Fauxquet.FauxquetObjs.{Encoding, RLE}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter

/**
  * Created by james on 1/26/17.
  */
class RunLengthBitPackingValuesWriter(encoder: RunLengthBitPackingHybridEncoder) extends ValuesWriter {
  override def bufferedSize(): Long = encoder.bufferedSize

  override def getAllocatedSize: Long = encoder.allocatedSize

  override def encoding: Encoding = RLE

  override def reset(): Unit = encoder.reset()

  override def writeInt(i: Int): Unit = encoder.writeInt(i)

  override def writeBoolean(b: Boolean): Unit = writeInt(if (b) 1 else 0)

  def close() = encoder.close()
}