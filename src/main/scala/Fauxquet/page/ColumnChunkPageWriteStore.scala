package main.scala.Fauxquet.page

import main.scala.Fauxquet.column.ColumnWriters.{ColumnChunkPageWriter, PageWriter}
import main.scala.Fauxquet.column.ColumnDescriptor
import main.scala.Fauxquet.flare.FauxquetFileWriter
import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 1/26/17.
  */
class ColumnChunkPageWriteStore(schema: MessageType) extends PageWriteStore {
  var writers = Map[ColumnDescriptor, ColumnChunkPageWriter]()

  for (path <- schema.columns()) {
    writers += (path -> new ColumnChunkPageWriter(path))
  }

  override def getPageWriter(path: ColumnDescriptor): PageWriter = writers(path)

  def flushToFileWriter(fauxquetFileWriter: FauxquetFileWriter): Unit = {
    for (path <- schema.columns()) {
      writers(path).writeToFileWriter(fauxquetFileWriter)
    }
  }
}