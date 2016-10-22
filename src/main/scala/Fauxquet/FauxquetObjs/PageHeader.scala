package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/30/16.
  */
class PageHeader extends Fauxquetable {
  var uncompressedPageSize = -1
  var compressedPageSize = -1
  var crc = -1 //wtf is this?

  var dataPageHeader: DataPageHeader = _
  var indexPageHeader: IndexPageHeader = _
  var dictionaryPageHeader: DictionaryPageHeader = _
  var dataPageHeaderV2: DataPageHeaderV2 = _

  var Type: PageType = _

  private val TYPE_FIELD_DESC = TField("type", 8, 1)
  private val UNCOMPRESSED_PAGE_SIZE_FIELD_DESC = TField("uncompressed_page_size", 8, 2)
  private val COMPRESSED_PAGE_SIZE_FIELD_DESC = TField("compressed_page_size", 8, 3)
  private val CRC_FIELD_DESC = TField("crc", 8, 4)
  private val DATA_PAGE_HEADER_FIELD_DESC = TField("data_page_header", 12, 5)
  private val INDEX_PAGE_HEADER_FIELD_DESC = TField("index_page_header", 12, 6)
  private val DICTIONARY_PAGE_HEADER_FIELD_DESC = TField("dictionary_page_header", 12, 7)
  private val DATA_PAGE_HEADER_V2_FIELD_DESC = TField("data_page_header_v2", 12, 8)

  override def className: String = "PageHeader"

  override def validate(): Unit = {
    if (uncompressedPageSize == -1)
      throw new Error("PageHeader uncompressedPageSize not found in file.")
    if (compressedPageSize == -1)
      throw new Error("PageHeader compressedPageSize not found in file.")
    if (Type == null)
      throw new Error("PageHeader Type not found in file.")
  }

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => Type = PageTypeManager.getPageTypeById(FauxquetDecoder readI32 arr)
      case 2 => uncompressedPageSize = FauxquetDecoder readI32 arr
      case 3 => compressedPageSize = FauxquetDecoder readI32 arr
      case 4 => crc = FauxquetDecoder readI32 arr
      case _ => FauxquetDecoder skip(arr, 8)
    }
    case TField(_, 12, x) => x match {
      case 5 =>
        dataPageHeader = new DataPageHeader()
        dataPageHeader read arr
      case 6 =>
        indexPageHeader = new IndexPageHeader()
        indexPageHeader read arr
      case 7 =>
        dictionaryPageHeader = new DictionaryPageHeader()
        dictionaryPageHeader read arr
      case 8 =>
        dataPageHeaderV2 = new DataPageHeaderV2()
        dataPageHeaderV2 read arr
      case _ => FauxquetDecoder skip(arr, 12)
    }
    case _ => FauxquetDecoder skip(arr, field.Type)
  }

  override def doWrite(): Unit = {
    def writeType(): Unit = {
      FauxquetEncoder writeFieldBegin TYPE_FIELD_DESC
      FauxquetEncoder writeI32 Type.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeUncompressedPageSize(): Unit = {
      FauxquetEncoder writeFieldBegin UNCOMPRESSED_PAGE_SIZE_FIELD_DESC
      FauxquetEncoder writeI32 uncompressedPageSize
      FauxquetEncoder writeFieldEnd()
    }

    def writeCompressedPageSize(): Unit = {
      FauxquetEncoder writeFieldBegin COMPRESSED_PAGE_SIZE_FIELD_DESC
      FauxquetEncoder writeI32 compressedPageSize
      FauxquetEncoder writeFieldEnd()
    }

    def writeCrc(): Unit = {
      FauxquetEncoder writeFieldBegin CRC_FIELD_DESC
      FauxquetEncoder writeI32 crc
      FauxquetEncoder writeFieldEnd()
    }

    def writeDataPageHeader(): Unit = {
      FauxquetEncoder writeFieldBegin DATA_PAGE_HEADER_FIELD_DESC
      dataPageHeader write()
      FauxquetEncoder writeFieldEnd()
    }

    def writeIndexPageHeader(): Unit = {
      FauxquetEncoder writeFieldBegin INDEX_PAGE_HEADER_FIELD_DESC
      indexPageHeader write()
      FauxquetEncoder writeFieldEnd()
    }

    def writeDictionaryPageHeader(): Unit = {
      FauxquetEncoder writeFieldBegin DICTIONARY_PAGE_HEADER_FIELD_DESC
      dictionaryPageHeader write()
      FauxquetEncoder writeFieldEnd()
    }

    def writeDataPageHeaderV2(): Unit = {
      FauxquetEncoder writeFieldBegin DATA_PAGE_HEADER_V2_FIELD_DESC
      dataPageHeaderV2 write()
      FauxquetEncoder writeFieldEnd()
    }

    if (this.Type != null) {
      writeType()
    }

    writeUncompressedPageSize()
    writeCompressedPageSize()

    if (crc != -1) {
      writeCrc()
    }

    if (dataPageHeader != null) {
      writeDataPageHeader()
    }

    if (indexPageHeader != null) {
      writeIndexPageHeader()
    }

    if (dictionaryPageHeader != null) {
      writeDictionaryPageHeader()
    }

    if (dataPageHeaderV2 != null) {
      writeDataPageHeaderV2()
    }
  }
}
