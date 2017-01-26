package main.scala.Fauxquet.ValuesWriters.bitpacking

import main.scala.Fauxquet.FauxquetObjs.{BIT_PACKED, Encoding}
import main.scala.Fauxquet.ValuesWriters.ValuesWriter

/**
  * Created by james on 1/26/17.
  */
class ByteBitPackingValuesWriter extends ValuesWriter {
  override def bufferedSize(): Long = ???

  //bunch of other stuff in Parquet -- do we need it?
  override def getAllocatedSize: Long = ???

  override def encoding: Encoding = BIT_PACKED


}
