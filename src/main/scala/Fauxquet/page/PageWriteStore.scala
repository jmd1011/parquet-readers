package main.scala.Fauxquet.page

import main.scala.Fauxquet.ColumnWriters.PageWriter
import main.scala.Fauxquet.FauxquetObjs.ColumnDescriptor

/**
  * Created by james on 1/26/17.
  */
trait PageWriteStore {
  def getPageWriter(path: ColumnDescriptor): PageWriter
}
