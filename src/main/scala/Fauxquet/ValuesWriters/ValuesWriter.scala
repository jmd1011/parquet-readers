package main.scala.Fauxquet.ValuesWriters

import main.scala.Fauxquet.FauxquetObjs.Encoding
import main.scala.Fauxquet.bytes.BytesInput.BytesInput

/**
  * Created by james on 1/25/17.
  */
abstract class ValuesWriter {
  def bufferedSize(): Long

  //bunch of other stuff in Parquet -- do we need it? turns out, yeah, we do
  def getAllocatedSize: Long = ???
  def encoding: Encoding = ???
  def reset(): Unit = ???
  def close(): Unit = ???
  def toBytes: BytesInput
  // end of other stuff

  def writeByte(value: Int): Unit = ???
  def writeBytes(bytes: Array[Byte]): Unit = ???
  def writeBoolean(b: Boolean): Unit = ???
  def writeInt(i: Int): Unit = ???
  def writeLong(l: Long): Unit = ???
  def writeDouble(d: Double): Unit = ???
  def writeFloat(f: Float): Unit = ???
}