package main.scala.Fauxquet.page

import main.scala.Fauxquet.column.ColumnWriters.PageWriter
import main.scala.Fauxquet.column.ColumnDescriptor

/**
  * Created by james on 1/26/17.
  */
trait PageWriteStore {
  def getPageWriter(path: ColumnDescriptor): PageWriter
}
