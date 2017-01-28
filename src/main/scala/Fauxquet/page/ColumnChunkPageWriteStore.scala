package main.scala.Fauxquet.page

import main.scala.Fauxquet.ColumnWriters.PageWriter
import main.scala.Fauxquet.FauxquetObjs.ColumnDescriptor
import main.scala.Fauxquet.bytes.Compressors.BytesCompressor

/**
  * Created by james on 1/26/17.
  */
class ColumnChunkPageWriteStore(path: ColumnDescriptor, compressor: BytesCompressor) extends PageWriteStore {
  override def getPageWriter(path: ColumnDescriptor): PageWriter = ???
}