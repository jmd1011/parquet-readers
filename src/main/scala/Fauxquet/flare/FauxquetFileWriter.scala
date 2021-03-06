package main.scala.Fauxquet.flare

import java.io.{BufferedOutputStream, FileOutputStream}
import java.nio.charset.Charset

import main.scala.Fauxquet.Encoders.PlainEncoder
import main.scala.Fauxquet.FauxquetObjs.ColumnChunkMetadata.ColumnChunkMetadataManager
import main.scala.Fauxquet.FauxquetObjs._
import main.scala.Fauxquet.FauxquetObjs.statistics.Statistics
import main.scala.Fauxquet._
import main.scala.Fauxquet.bytes.BytesInput.BytesInput
import main.scala.Fauxquet.column.ColumnDescriptor
import main.scala.Fauxquet.flare.metadata.{ColumnPath, ColumnPathGetter, FauxquetMetadataConverter}
import main.scala.Fauxquet.page.DictionaryPage
import main.scala.Fauxquet.schema.{MessageType, PrimitiveTypeName}

/**
  * Created by james on 1/27/17.
  */
class FauxquetFileWriter(path: String, schema: MessageType, alignmentStrategy: AlignmentStrategy = NoAlignment) {
  //block data
  var blocks = List[BlockMetadata]()
  var currentBlock = new BlockMetadata()

  var currentRecordCount: Long = -1
  var uncompressedLength: Long = -1
  var compressedLength: Long   = -1
  //end block data

  val out = new FauxquetOutputStream(new BufferedOutputStream(new FileOutputStream(path), 4096))

  def getNextRowGroupSize: Long = alignmentStrategy.nextRowGroupSize(out)

  def pos = out.pos

  //column data

  //Do we need these? Hopefully not...
//  private CompressionCodecName currentChunkCodec; // set in startColumn
//  private ColumnPath currentChunkPath;            // set in startColumn
//  private PrimitiveTypeName currentChunkType;     // set in startColumn
  var currentChunkPath: ColumnPath           = _
  var currentChunkType: PrimitiveTypeName    = _
  var currentChunkValueCount: Long           = -1
  var currentChunkFirstDataPage: Long        = -1
  var currentChunkDictionaryPageOffset: Long = -1
  //end column data

  val MAGIC = "PAR1".getBytes(Charset.forName("ASCII"))

  var state: WriteState = NOT_STARTED
  var encodings: Set[Encoding] = Set[Encoding]()

  def start() = {
    state = state.start()
    out.write(MAGIC)
  }

  def startBlock(recordCount: Long) = {
    state = state.startBlock()
    alignmentStrategy.alignForRowGroup(out)

    currentBlock = new BlockMetadata()
    currentRecordCount = recordCount
  }

  def startColumn(descriptor: ColumnDescriptor, valueCount: Long): Unit = {
    state = state.startColumn()

    currentChunkPath = ColumnPathGetter.get(descriptor.path)
    currentChunkType = descriptor.primitive
    currentChunkValueCount = valueCount
    currentChunkFirstDataPage = out.pos

    compressedLength = 0
    uncompressedLength = 0
  }

  //this won't get called
  def writeDictionaryPage(dictionaryPage: DictionaryPage): Unit = {
    state = state.write()

    currentChunkDictionaryPageOffset = out.pos
    val uncompressedSize: Int = dictionaryPage.uncompressedSize
    val compressedPageSize: Int = dictionaryPage.bytes.size.asInstanceOf[Int]

    val pageHeader = new PageHeader(DICTIONARY_PAGE, uncompressedLength.asInstanceOf[Int], compressedLength.asInstanceOf[Int])
    pageHeader.dictionaryPageHeader = new DictionaryPageHeader()
    pageHeader.write(new PlainEncoder(out))

    val headerSize = out.pos - currentChunkDictionaryPageOffset
    this.uncompressedLength += uncompressedSize + headerSize
    this.compressedLength += compressedPageSize + headerSize

    dictionaryPage.bytes.writeAllTo(out)
    encodings += dictionaryPage.encoding
  }

  //probably won't get called
  def writeDataPage(valueCount: Int, uncompressedPageSize: Int, bytes: BytesInput, statistics: Statistics, rlEncoding: Encoding, dlEncoding: Encoding, valuesEncoding: Encoding): Unit = {
    state = state.write()

    val beforeHeader = out.pos
    val compressedPageSize = bytes.size

    val pageHeader = new PageHeader(DATA_PAGE, uncompressedPageSize, compressedPageSize.asInstanceOf[Int])
    pageHeader.dataPageHeader = new DataPageHeader(valueCount, valuesEncoding, dlEncoding, rlEncoding, statistics)
    pageHeader.write(new PlainEncoder(out), reset = true)

    val headerSize = out.pos - beforeHeader
    this.uncompressedLength += uncompressedPageSize + headerSize
    this.compressedLength += compressedPageSize + headerSize

    bytes.writeAllTo(out)
    encodings += rlEncoding
    encodings += dlEncoding
    encodings += valuesEncoding
  }

  //will get called (hopefully)
  def writeDataPages(bytes: BytesInput, uncompressedTotalPageSize: Long, compressedTotalPageSize: Long, totalStats: Statistics,
                     rlEncodings: Set[Encoding], dlEncodings: Set[Encoding], dataEncodings: Set[Encoding]): Unit = {
    state = state.write()

    val headerSize = bytes.size - compressedTotalPageSize
    this.uncompressedLength += uncompressedTotalPageSize + headerSize
    this.compressedLength += compressedTotalPageSize + headerSize

    bytes.writeAllTo(out)
    encodings ++= rlEncodings ++ dlEncodings ++ dataEncodings
  }

  def endColumn(): Unit = {
    state = state.endColumn()

    currentBlock.addColumn(ColumnChunkMetadataManager.get(currentChunkPath, currentChunkType, currentChunkFirstDataPage, currentChunkDictionaryPageOffset, currentChunkValueCount, compressedLength, uncompressedLength))
    currentBlock.totalBytesSize += uncompressedLength
    uncompressedLength = 0
    compressedLength = 0
  }

  def endBlock(): Unit = {
    state = state.endBlock()

    currentBlock.rowCount = currentRecordCount
    blocks :+= currentBlock
    currentBlock = null
  }

  def end(extraMetadata: Map[String, String]): Unit = {
    state = state.end()

    val footer = new FauxquetMetadata(new FileMetadata(schema, extraMetadata), blocks)
    serializeFooter(footer)
    out.close()
  }

  def serializeFooter(footer: FauxquetMetadata): Unit = {
    val footerIndex = out.pos
    val metadata: FileMetadata = FauxquetMetadataConverter.fromFauxquetMetadata(footer, schema)
    writeMetadata(metadata)
    writeIntLittleEndian((out.pos - footerIndex).asInstanceOf[Int])
    out.write(MAGIC)
  }

  def writeMetadata(metadata: FileMetadata): Unit = {
    metadata.write(new PlainEncoder(out), reset = true)
  }

  def writeIntLittleEndian(v: Int): Unit = {
    this.out.write((v >>> 0) & 0xFF)
    this.out.write((v >>> 8) & 0xFF)
    this.out.write((v >>> 16) & 0xFF)
    this.out.write((v >>> 24) & 0xFF)
  }

  def getType(primitiveTypeName: PrimitiveTypeName): TType = primitiveTypeName match {
    case main.scala.Fauxquet.schema.INT32 => INT32
    case main.scala.Fauxquet.schema.INT64 => INT64
    case main.scala.Fauxquet.schema.BOOLEAN => BOOLEAN
    case main.scala.Fauxquet.schema.BINARY => BYTE_ARRAY
    case main.scala.Fauxquet.schema.FLOAT => FLOAT
    case main.scala.Fauxquet.schema.DOUBLE => DOUBLE
    case main.scala.Fauxquet.schema.INT96 => INT96
    case main.scala.Fauxquet.schema.FIXED_LEN_BYTE_ARRAY => FIXED_LEN_BYTE_ARRAY
    case _ => throw new Error("Unknown type encountered")
  }
}
