package main.scala.Fauxquet.flare

import main.scala.Fauxquet.column.impl.ColumnWriteStoreImpl
import main.scala.Fauxquet.io.MessageColumnIO
import main.scala.Fauxquet.page.ColumnChunkPageWriteStore
import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 1/27/17.
  */
class InternalFauxquetRecordWriter(val fauxquetFileWriter: FauxquetFileWriter, val schema: MessageType, val extraMetadata: Map[String, String], val rowGroupSize: Long) {
  val pageStore = new ColumnChunkPageWriteStore(schema)
  val columnStore = new ColumnWriteStoreImpl(pageStore)
  val recordConsumer = new MessageColumnIO(schema, false, "James Decker").getRecordWriter(columnStore)
}