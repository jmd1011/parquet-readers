package main.scala.Fauxquet.column.impl

import main.scala.Fauxquet.column.ColumnWriters.{ColumnWriter, ColumnWriterImpl}
import main.scala.Fauxquet.column.{ColumnDescriptor, ColumnWriteStore}
import main.scala.Fauxquet.page.PageWriteStore

/**
  * Created by james on 1/30/17.
  */
class ColumnWriteStoreImpl(val pageWriteStore: PageWriteStore) extends ColumnWriteStore {
  var columns = Map[ColumnDescriptor, ColumnWriterImpl]()

  override def getColumnWriter(path: ColumnDescriptor): ColumnWriter = {
    val column = columns.get(path)

    column match {
      case Some(col) => col
      case None => val nCol = new ColumnWriterImpl(path, pageWriteStore.getPageWriter(path))
        columns += (path -> nCol)

        nCol
      case _ => throw new Error("This is literally impossible")
    }
  }

  override def flush(): Unit = columns.values.foreach(_.flush())

  override def endRecord(): Unit = { } //intentionally left blank

  override def allocatedSize(): Long = (0L /: columns.values) (_ + _.allocatedSize)

  override def bufferedSize(): Long = (0L /: columns.values) (_ + _.bufferedSize)

  def maxColMemSize: Long = { //TODO: More scala way to do this?
    var max = 0L

    for (v <- columns.values) {
      max = math.max(max, v.bufferedSize)
    }

    max
  }

  override def close(): Unit = columns.values.foreach(_.close())
}
