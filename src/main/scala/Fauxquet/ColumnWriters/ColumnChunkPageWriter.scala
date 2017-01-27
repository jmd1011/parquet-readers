package main.scala.Fauxquet.ColumnWriters
import java.io.ByteArrayOutputStream

import main.scala.Fauxquet.Encoders.PlainEncoder
import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput, BytesInputManager, ConcatenatingByteArrayCollector}
import main.scala.Fauxquet.page.DictionaryPage

/**
  * Created by james on 1/27/17.
  */
class ColumnChunkPageWriter(path: ColumnDescriptor) extends PageWriter {
  val tempOutputStream = new ByteArrayOutputStream()
  val buf = new ConcatenatingByteArrayCollector()
  var dictionaryPage: DictionaryPage = _

  var uncompressedLength: Long = 0L
  var compressedLength: Long = 0L
  var totalValueCount: Long = 0L
  var pageCount: Int = 0

  val statistics = new Statistics()

                                                                                     //these will be RLE                         //this will be PLAIN
  override def writePage(bytes: BytesInput, valueCount: Int, statistics: Statistics, rlEncoding: Encoding, dlEncoding: Encoding, valuesEncoding: Encoding): Unit = {
    val uncompressedSize = bytes.size

    if (uncompressedSize > Int.MaxValue) {
      throw new Error("This page is over 9000!!!!!")
    }

    tempOutputStream.reset()

    val pageHeader = new PageHeader(DATA_PAGE, uncompressedLength.asInstanceOf[Int], compressedLength.asInstanceOf[Int])
    pageHeader.dataPageHeader = new DataPageHeader(valueCount, valuesEncoding, dlEncoding, rlEncoding, statistics)
    pageHeader.write(new PlainEncoder(tempOutputStream)) //This will be handled by FauxquetEncoder -- just need to provide another encoder to wrap the stream

    this.uncompressedLength += uncompressedSize
    this.compressedLength += bytes.size //compressed and uncompressed are the same for IHCP
    this.totalValueCount += valueCount
    this.pageCount += 1
    //merge statistics
    buf.collect(BytesInputManager.concat(BytesInputManager.from(tempOutputStream), bytes))
  }

  override def memSize: Long = ???

  override def allocatedSize: Long = ???

  override def writeDictionaryPage(dictionaryPage: DictionaryPage): Unit = ???
}
