package main.scala.Fauxquet.column

import main.scala.Fauxquet.column.ColumnWriters.ColumnWriter

/**
  * Created by james on 1/28/17.
  */
trait ColumnWriteStore { //TODO: Duplicated fields across ColumnWriter?
  def getColumnWriter(path: ColumnDescriptor): ColumnWriter
  def flush(): Unit
  def endRecord(): Unit
  def allocatedSize(): Long
  def bufferedSize(): Long
  def close(): Unit
}
