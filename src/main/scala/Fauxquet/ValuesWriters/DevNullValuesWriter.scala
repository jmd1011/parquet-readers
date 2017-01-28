package main.scala.Fauxquet.ValuesWriters
import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, Encoding}
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput, EmptyBytesInput}

/**
  * Created by james on 1/26/17.
  */
object DevNullValuesWriter extends ValuesWriter {
  override def reset(): Unit = {}

  override def close(): Unit = {}

  override def bufferedSize(): Long = 0L

  override def writeBoolean(b: Boolean): Unit = {}

  override def writeByte(value: Int): Unit = {}

  override def writeBytes(bytes: Array[Byte]): Unit = {}

  override def encoding: Encoding = BIT_PACKED

  override def getAllocatedSize: Long = 0L

  override def writeDouble(d: Double): Unit = {}

  override def writeFloat(f: Float): Unit = {}

  override def writeInt(i: Int): Unit = {}

  override def writeLong(l: Long): Unit = {}

  override def toBytes(): BytesInput = EmptyBytesInput
}
