package main.scala.Fauxquet

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

  override def className: String = "PageHeader"

  override def write(): Unit = ???

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
}
