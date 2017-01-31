package main.scala.Fauxquet.flare

import main.scala.Fauxquet.column.ColumnWriteStore
import main.scala.Fauxquet.column.impl.ColumnWriteStoreImpl
import main.scala.Fauxquet.flare.api.WriteSupport
import main.scala.Fauxquet.io.MessageColumnIO
import main.scala.Fauxquet.io.api.RecordConsumer
import main.scala.Fauxquet.page.ColumnChunkPageWriteStore
import main.scala.Fauxquet.schema.MessageType

/**
  * Created by james on 1/27/17.
  */
class InternalFauxquetRecordWriter(val fauxquetFileWriter: FauxquetFileWriter, val writeSupport: WriteSupport, val schema: MessageType, val extraMetadata: Map[String, String], val rowGroupSize: Long) {
  var pageStore: ColumnChunkPageWriteStore = _
  var columnStore: ColumnWriteStore = _
  var recordConsumer: RecordConsumer = _
  var closed: Boolean = false

  val MINIMUM_RECORD_COUNT_FOR_CHECK = 100
  val MAXIMUM_RECORD_COUNT_FOR_CHECK = 10000

  var recordCount: Long = 0L
  var recordCountForNextMemCheck: Long = MINIMUM_RECORD_COUNT_FOR_CHECK
  var lastRowGroupEndPos = 0L
  var nextRowGroupSize = rowGroupSize

  def init(): Unit = {
    pageStore = new ColumnChunkPageWriteStore(schema)
    columnStore = new ColumnWriteStoreImpl(pageStore)
    recordConsumer = new MessageColumnIO(schema, false, "Flare Team").getRecordWriter(columnStore)
    writeSupport.prepareForWrite(recordConsumer)
  }

  def write(values: List[String]): Unit = {
    writeSupport.write(values)
    recordCount += 1
    checkBlockSizeReached()
  }

  def checkBlockSizeReached(): Unit = {
    if (recordCount >= recordCountForNextMemCheck) {
      val memSize = columnStore.bufferedSize()
      val recordSize = memSize / recordCount

      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        flushRowGroupToStore()
        init()
        recordCountForNextMemCheck = math.min(math.max(MINIMUM_RECORD_COUNT_FOR_CHECK, recordCount / 2), MAXIMUM_RECORD_COUNT_FOR_CHECK)
        lastRowGroupEndPos = fauxquetFileWriter.pos
      } else {
        recordCountForNextMemCheck = math.min(math.max(MINIMUM_RECORD_COUNT_FOR_CHECK, (recordCount + (nextRowGroupSize / recordSize.asInstanceOf[Float]).asInstanceOf[Long]) / 2),
                                              recordCount + MAXIMUM_RECORD_COUNT_FOR_CHECK)
      }
    }
  }

  def flushRowGroupToStore(): Unit = {
    recordConsumer.flush()

    if (recordCount > 0) {
      fauxquetFileWriter.startBlock(recordCount)
      columnStore.flush()
      pageStore.flushToFileWriter(fauxquetFileWriter)
      recordCount = 0
      fauxquetFileWriter.endBlock()

      nextRowGroupSize = math.min(fauxquetFileWriter.getNextRowGroupSize, rowGroupSize)
    }

    columnStore = null
    pageStore = null
  }

  def close(): Unit = {
    if (!closed) {
      flushRowGroupToStore()
      val finalMetadata = extraMetadata + ("writer.model.name" -> writeSupport.name)
      fauxquetFileWriter.end(finalMetadata)
      closed = true
    }
  }

  init()
}