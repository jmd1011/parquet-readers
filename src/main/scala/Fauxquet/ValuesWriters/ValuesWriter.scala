package main.scala.Fauxquet.ValuesWriters

import main.scala.Fauxquet.FauxquetObjs.Encoding

/**
  * Created by james on 1/25/17.
  */
abstract class ValuesWriter {
  def bufferedSize(): Long

  //bunch of other stuff in Parquet -- do we need it?
  def getAllocatedSize: Long = ???
  def encoding: Encoding = ???
  def reset(): Unit = ???
  def close(): Unit = ???
  // end of other stuff

  def writeByte(value: Int): Unit = ???
  def writeBytes(bytes: Array[Byte]): Unit = ???
  def writeBoolean(b: Boolean): Unit = ???
  def writeInt(i: Int): Unit = ???
  def writeLong(l: Long): Unit = ???
  def writeDouble(d: Double): Unit = ???
  def writeFloat(f: Float): Unit = ???
}