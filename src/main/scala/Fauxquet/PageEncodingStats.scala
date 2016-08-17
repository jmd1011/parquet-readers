package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class PageEncodingStats extends Fauxquetable {
  var count: Int = -1

  var encoding: Encoding = _
  var pageType: PageType = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => pageType = PageTypeManager getPageTypeById(FauxquetDecoder readI32 arr)
      case 2 => encoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 3 => count = FauxquetDecoder readI32 arr
      case _ => FauxquetDecoder skip(arr, 8)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def write(): Unit = ???

  override def validate(): Unit = {
    if (count == -1) throw new Error("PageEncodingStats count was not found in file.")
    if (encoding == null) throw new Error("PageEncodingStats encoding was not found in file.")
    if (pageType == null) throw new Error("PageEncodingStats pageType was not found in file.")
  }

  override def className: String = "PageEncodingStats"
}
