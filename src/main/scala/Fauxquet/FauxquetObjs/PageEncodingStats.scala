package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class PageEncodingStats extends Fauxquetable {
  var count: Int = -1

  var encoding: Encoding = _
  var pageType: PageType = _

  override def className: String = "PageEncodingStats"

  private val PAGE_TYPE_FIELD_DESC = TField("page_type", 8, 1)
  private val ENCODING_FIELD_DESC = TField("encoding", 8, 2)

  private val COUNT_FIELD_DESC = TField("count", 8, 3)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => pageType = PageTypeManager getPageTypeById(FauxquetDecoder readI32 arr)
      case 2 => encoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 3 => count = FauxquetDecoder readI32 arr
      case _ => FauxquetDecoder skip(arr, 8)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writePageType(): Unit = {
      FauxquetEncoder writeFieldBegin PAGE_TYPE_FIELD_DESC
      FauxquetEncoder writeI32 pageType.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeEncoding(): Unit = {
      FauxquetEncoder writeFieldBegin ENCODING_FIELD_DESC
      FauxquetEncoder writeI32 encoding.id
      FauxquetEncoder writeFieldEnd()
    }

    def writeCount(): Unit = {
      FauxquetEncoder writeFieldBegin COUNT_FIELD_DESC
      FauxquetEncoder writeI32 count
      FauxquetEncoder writeFieldEnd()
    }

    if (this.pageType != null) {
      writePageType()
    }

    if (this.encoding != null) {
      writeEncoding()
    }

    writeCount()
  }

  override def validate(): Unit = {
    if (count == -1) throw new Error("PageEncodingStats count was not found in file.")
    if (encoding == null) throw new Error("PageEncodingStats encoding was not found in file.")
    if (pageType == null) throw new Error("PageEncodingStats pageType was not found in file.")
  }
}
