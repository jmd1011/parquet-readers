package main.scala.Fauxquet.ValuesWriters.plain

import main.scala.Fauxquet.Encoders.LittleEndianEncoder
import main.scala.Fauxquet.FauxquetObjs.{Encoding, PLAIN}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput}
import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream
import main.scala.Fauxquet.io.api.Binary

/**
  * Created by james on 1/25/17.
  */
class PlainValuesWriter(capacityByteArrayOutputStream: CapacityByteArrayOutputStream) extends ValuesWriter {
  val out = new LittleEndianEncoder(capacityByteArrayOutputStream)

  override def bufferedSize(): Long = capacityByteArrayOutputStream.size

  override def writeByte(value: Int): Unit = throw new Error("PlainValuesWriter.writeByte not allowed (don't ask)")

  override def writeBytes(bytes: Binary): Unit = {
    out.writeInt(bytes.length)
    bytes.writeTo(out)
  }

  override def writeBoolean(b: Boolean): Unit = out.writeBoolean(b)

  override def writeInt(i: Int): Unit = out.writeInt(i)

  override def writeLong(l: Long): Unit = out.writeLong(l)

  override def writeDouble(d: Double): Unit = out.writeDouble(d)

  override def writeFloat(f: Float): Unit = out.writeFloat(f)

  override def getAllocatedSize: Long = capacityByteArrayOutputStream.bytesAllocated

  override def reset() = capacityByteArrayOutputStream.reset()

  override def encoding: Encoding = PLAIN

  override def close(): Unit = {
    capacityByteArrayOutputStream.close()
    out.close()
  }

  override def toBytes: BytesInput = {
    out.flush()
    BytesInput.from(capacityByteArrayOutputStream)
  }
}
