package main.scala.Fauxquet.ValuesWriters

/**
  * Created by james on 1/25/17.
  */
abstract class ValuesWriter {
  def bufferedSize(): Long

  //bunch of other stuff in Parquet -- do we need it?
  def getAllocatedSize: Long
  // end of other stuff

  def writeByte(value: Int)
  def writeBytes(bytes: Array[Byte])
  def writeBoolean(b: Boolean)
  def writeInt(i: Int)
  def writeLong(l: Long)
  def writeDouble(d: Double)
  def writeFloat(f: Float)
}