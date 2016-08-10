package main.scala.Fauxquet

/**
  * Created by james on 8/9/16.
  */
class ColumnMetadata extends Fauxquetable {
  var numValues: Long = -1L

  var totalUncompressedSize: Long = -1L
  var totalCompressedSize: Long = -1L

  var dataPageOffset: Long = -1L
  var indexPageOffset: Long = -1L
  var dictionaryPageOffset: Long = -1L

  var Type: TType = _

  var encodings: List[Encoding] = Nil
  var pathInSchema: List[String] = Nil
  var encodingStats: List[PageEncodingStats] = Nil

  var codec: CompressionCodec = _
  var statistics: Statistics = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => Type = TTypeManager.getType(FauxquetDecoder readI32 arr)
      case 4 => codec = CompressionCodecManager.getCodecById(FauxquetDecoder readI32 arr)
    }
    case TField(_, 15, x) => x match {
      case 2 =>
        val list = FauxquetDecoder readListBegin arr

        for (i <- 0 until list.size) encodings ::= {
          val x = FauxquetDecoder readI32 arr
          val e = EncodingManager getEncodingById x
          e
        }
      case 3 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) pathInSchema ::= FauxquetDecoder.readString(arr)
      case 8 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) keyValueMetadata ::= {
          val kv = new KeyValue
          kv read arr
          kv
        }
      case 13 =>
        val list = FauxquetDecoder readListBegin arr

        for (i <- 0 until list.size) encodingStats ::= {
          val pes = new PageEncodingStats
          pes read arr
          pes
        }
      case _ => FauxquetDecoder skip(arr, 15)
    }
    case TField(_, 10, x) => x match {
      case 5 => numValues = FauxquetDecoder readI64 arr
      case 6 => totalUncompressedSize = FauxquetDecoder readI64 arr
      case 7 => totalCompressedSize = FauxquetDecoder readI64 arr
      case 9 => dataPageOffset = FauxquetDecoder readI64 arr
      case 10 => indexPageOffset = FauxquetDecoder readI64 arr
      case 11 => dictionaryPageOffset = FauxquetDecoder readI64 arr
    }
    case TField(_, 12, 12) => statistics = {val s = new Statistics; s read arr; s}
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {
    if (numValues == -1L) throw new Error("ColumnMetadata numValues was not found in file.")
    if (totalUncompressedSize == -1L) throw new Error("ColumnMetadata totalUncompressedSize was not found in file.")
    if (totalCompressedSize == -1L) throw new Error("ColumnMetadata totalCompressedSize was not found in file.")
    if (dataPageOffset == -1L) throw new Error("ColumnMetadata dataPageOffset was not found in file.")
  }
}
