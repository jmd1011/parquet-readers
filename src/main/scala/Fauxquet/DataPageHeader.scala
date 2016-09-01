package main.scala.Fauxquet

/**
  * Created by james on 8/30/16.
  */
class DataPageHeader extends Fauxquetable {
  var numValues = -1

  var encoding: Encoding = _
  var definitionLevelEncoding: Encoding = _
  var repetitionLevelEncoding: Encoding = _

  var statistics: Statistics = _

  override def className: String = "DataPageHeader"

  override def write(): Unit = ???

  override def validate(): Unit = {
    if (numValues == -1) throw new Error("DataPageHeader numValues was not found in file.")
    if (encoding == null) throw new Error("DataPageHeader encoding was not found in file.")
    if (definitionLevelEncoding == null) throw new Error("DataPageHeader definitionLevelEncoding was not found in file.")
    if (repetitionLevelEncoding == null) throw new Error("DataPageHeader repetitionLevelEncoding was not found in file.")
  }

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => numValues = FauxquetDecoder readI32 arr
      case 2 => encoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 3 => definitionLevelEncoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case 4 => repetitionLevelEncoding = EncodingManager getEncodingById(FauxquetDecoder readI32 arr)
      case _ => FauxquetDecoder skip(arr, 8)
    }
    case TField(_, 12, 5) =>
      statistics = new Statistics()
      statistics read arr
    case _ => FauxquetDecoder skip(arr, field Type)
  }
}
