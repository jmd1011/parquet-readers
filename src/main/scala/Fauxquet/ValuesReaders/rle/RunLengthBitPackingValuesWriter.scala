package main.scala.Fauxquet.ValuesReaders.rle

import java.io.ByteArrayOutputStream

import main.scala.Fauxquet.Encoders.RunLengthBitPackingHybridEncoder
import main.scala.Fauxquet.FauxquetObjs.{Encoding, RLE}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter
import main.scala.Fauxquet.bytes.BytesInput.BytesInput

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

  override def close() = encoder.close()

  val length = new ByteArrayOutputStream(4)

  override def toBytes: BytesInput = {
    val rle = this.encoder.toBytes
    writeLength(rle.size.asInstanceOf[Int])
    BytesInput.concat(BytesInput.from(length), rle) //TODO: put this back the way it was if this doesn't work
  }

  private def writeLength(v: Int): Unit = {
    this.length.write((v >>> 0) & 0xFF)
    this.length.write((v >>> 8) & 0xFF)
    this.length.write((v >>> 16) & 0xFF)
    this.length.write((v >>> 24) & 0xFF)
  }
}