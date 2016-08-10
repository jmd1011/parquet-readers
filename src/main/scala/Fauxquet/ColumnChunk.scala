package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class ColumnChunk extends Fauxquetable {
  var fileOffset: Long = -1L
  var filePath: String = _

  var metadata: ColumnMetadata = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 11, 1) => filePath = FauxquetDecoder readString arr
    case TField(_, 10, 2) => fileOffset = FauxquetDecoder readI64 arr
    case TField(_, 12, 3) => metadata = {
      val md = new ColumnMetadata
      md read arr
      md
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {
    if (fileOffset == -1L) throw new Error("ColumnChunk fileOffset was not found in file.")
  }
}
