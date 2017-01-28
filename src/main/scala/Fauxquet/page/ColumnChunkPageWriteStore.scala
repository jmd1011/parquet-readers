package main.scala.Fauxquet.page

import main.scala.Fauxquet.column.ColumnWriters.PageWriter
import main.scala.Fauxquet.bytes.Compressors.BytesCompressor
import main.scala.Fauxquet.column.ColumnDescriptor

/**
  * Created by james on 1/26/17.
  */
class ColumnChunkPageWriteStore(path: ColumnDescriptor, compressor: BytesCompressor) extends PageWriteStore {
  override def getPageWriter(path: ColumnDescriptor): PageWriter = ???
}