package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class RowGroup extends Fauxquetable {
  var totalByteSize: Long = -1L
  var numRows: Long = -1L

  var columns: List[ColumnChunk] = Nil
  var sortingColumns: List[SortingColumn] = Nil

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 15, x) => x match {
      case 1 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) columns ::= {
          val cc = new ColumnChunk
          cc read arr
          cc
        }
      case 4 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) sortingColumns ::= {
          val sc = new SortingColumn
          sc read arr
          sc
        }
      case _ => FauxquetDecoder skip(arr, field Type)
    }
    case TField(_, 10, x) => x match {
      case 2 => totalByteSize = FauxquetDecoder readI64 arr
      case 3 => numRows = FauxquetDecoder readI64 arr
      case _ => FauxquetDecoder skip(arr, field Type)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {
    if (totalByteSize == -1L) throw new Error("RowGroup totalByteSize not found in file.")
    if (numRows == -1L) throw new Error("RowGroup numRows not found in file.")
    if (columns == null) throw new Error("RowGroup columns not found in file.")
  }
}
