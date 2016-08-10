package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class SortingColumn extends Fauxquetable {
  var columnIndex: Int = -1
  var descending: Option[Boolean] = _
  var nullsFirst: Option[Boolean] = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, 1) => columnIndex = FauxquetDecoder readI32 arr
    case TField(_, 2, x) => x match {
      case 2 => descending = Option(FauxquetDecoder readBool arr)
      case 3 => nullsFirst = Option(FauxquetDecoder readBool arr)
      case _ => FauxquetDecoder skip(arr, 2)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {
    if (columnIndex == -1) throw new Error("SortingColumn columnIndex not found in file")
    if (descending isEmpty) throw new Error("SortingColumn descending not found in file.")
    if (nullsFirst isEmpty) throw new Error("SortingColumn nullsFirst not found in file.")
  }
}
