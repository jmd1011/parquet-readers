package main.scala.Fauxquet.column.ColumnWriters
import java.io.ByteArrayOutputStream

import main.scala.Fauxquet.Encoders.PlainEncoder
import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.FauxquetObjs.statistics.Statistics
import main.scala.Fauxquet.bytes.BytesInput.{BytesInput, ConcatenatingByteArrayCollector}
import main.scala.Fauxquet.bytes.CapacityByteArrayOutputStream
import main.scala.Fauxquet.column.ColumnDescriptor
import main.scala.Fauxquet.flare.FauxquetFileWriter
import main.scala.Fauxquet.page.DictionaryPage

/**
  * Created by james on 1/27/17.
  */
class ColumnChunkPageWriter(val path: ColumnDescriptor, initialSlabSize: Int) extends PageWriter {
  val tempOutputStream = new ByteArrayOutputStream()
  val buf = new ConcatenatingByteArrayCollector() //new CapacityByteArrayOutputStream(initialSlabSize)
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

    val pageHeader = new PageHeader(DATA_PAGE, uncompressedSize.asInstanceOf[Int], uncompressedSize.asInstanceOf[Int])
    pageHeader.dataPageHeader = new DataPageHeader(valueCount, valuesEncoding, dlEncoding, rlEncoding, statistics)
    pageHeader.write(new PlainEncoder(tempOutputStream)) //This will be handled by FauxquetEncoder -- just need to provide another encoder to wrap the stream

    this.uncompressedLength += uncompressedSize
    this.compressedLength += bytes.size //compressed and uncompressed are the same for IHCP
    this.totalValueCount += valueCount
    this.pageCount += 1

    //bytes.writeAllTo(buf)

    //merge statistics
    buf.collect(BytesInput.concat(BytesInput.from(tempOutputStream), bytes))
  }

  def writeToFileWriter(fauxquetFileWriter: FauxquetFileWriter): Unit = {
    fauxquetFileWriter.startColumn(path, totalValueCount)

    if (dictionaryPage != null) {
      fauxquetFileWriter.writeDictionaryPage(dictionaryPage)
    }

    fauxquetFileWriter.writeDataPages(buf, uncompressedLength, compressedLength, statistics, Set[Encoding](BIT_PACKED), Set[Encoding](RLE), Set[Encoding](PLAIN))
    fauxquetFileWriter.endColumn()

    pageCount = 0
  }

  override def memSize: Long = buf.size

  override def allocatedSize: Long = buf.size

  override def writeDictionaryPage(dictionaryPage: DictionaryPage): Unit = {
    if (this.dictionaryPage != null) {
      throw new Error("Too many dictionary pages!")
    }

    val dictionaryBytes = dictionaryPage.bytes
    this.dictionaryPage = new DictionaryPage(BytesInput.copy(dictionaryBytes), dictionaryPage.dictionarySize, dictionaryPage.encoding)
  }
}
