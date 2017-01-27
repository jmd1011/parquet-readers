package main.scala.Fauxquet.page
import java.io.ByteArrayOutputStream

import main.scala.Fauxquet.FauxquetObjs.ColumnDescriptor
import main.scala.Fauxquet.bytes.BytesInput.ConcatenatingByteArrayCollector
import main.scala.Fauxquet.bytes.Compressors.BytesCompressor

/**
  * Created by james on 1/26/17.
  */
class ColumnChunkPageWriteStore(path: ColumnDescriptor, compressor: BytesCompressor) extends PageWriteStore {
  val tempOutputStream = new ByteArrayOutputStream()
  val buf = new ConcatenatingByteArrayCollector()
  var dictionaryPage: DictionaryPage = _

  var uncompressedLength: Long = 0L
  var compressedLength: Long = 0L
  var totalValueCount: Long = 0L
  var pageCount: Int = 0






  override def getPageWriter(path: ColumnDescriptor): Unit = ???
}