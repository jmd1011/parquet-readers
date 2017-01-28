package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class ColumnMetadata(var Type: TType = null, var encodings: List[Encoding] = Nil, var pathInSchema: List[String] = Nil, var codec: CompressionCodec = UNCOMPRESSED,
                     var numValues: Long = -1L, var totalUncompressedSize: Long = -1, var totalCompressedSize: Long = -1L, var dataPageOffset: Long = -1L
                    ) extends Fauxquetable {

  var indexPageOffset: Long = -1L
  var dictionaryPageOffset: Long = -1L

  var encodingStats: List[PageEncodingStats] = Nil

  var statistics: Statistics = _

  private val TYPE_FIELD_DESC = TField("type", 8, 1)
  private val ENCODINGS_FIELD_DESC = TField("encodings", 15, 2)
  private val PATH_IN_SCHEMA_FIELD_DESC = TField("path_in_schema", 15, 3)
  private val CODEC_FIELD_DESC = TField("codec", 8, 4)
  private val NUM_VALUES_FIELD_DESC = TField("num_values", 10, 5)
  private val TOTAL_UNCOMPRESSED_SIZE_FIELD_DESC = TField("total_uncompressed_size", 10, 6)
  private val TOTAL_COMPRESSED_SIZE_FIELD_DESC = TField("total_compressed_size", 10, 7)
  private  val KEY_VALUE_METADATA_FIELD_DESC = TField("key_value_metadata", 15, 8)
  private val DATA_PAGE_OFFSET_FIELD_DESC = TField("data_page_offset", 10, 9)
  private val INDEX_PAGE_OFFSET_FIELD_DESC = TField("index_page_offset", 10, 10)
  private val DICTIONARY_PAGE_OFFSET_FIELD_DESC = TField("dictionary_page_offset", 10, 11)
  private val STATISTICS_FIELD_DESC = TField("statistics", 12, 12)
  private val ENCODING_STATS_FIELD_DESC = TField("encoding_stats", 15, 13)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, x) => x match {
      case 1 => Type = TTypeManager.getType(FauxquetDecoder readI32 arr)
      case 4 => codec = CompressionCodecManager.getCodecById(FauxquetDecoder readI32 arr)
    }
    case TField(_, 15, x) => x match {
      case 2 =>
        val list = FauxquetDecoder readListBegin arr

        for (i <- 0 until list.size) encodings :+= {
          val x = FauxquetDecoder readI32 arr
          val e = EncodingManager getEncodingById x
          e
        }
      case 3 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) pathInSchema :+= FauxquetDecoder.readString(arr)
      case 8 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) keyValueMetadata :+= {
          val kv = new KeyValue
          kv read arr
          kv
        }
      case 13 =>
        val list = FauxquetDecoder readListBegin arr

        for (i <- 0 until list.size) encodingStats :+= {
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
      case _ => FauxquetDecoder skip(arr, 10)
    }
    case TField(_, 12, 12) => statistics = {val s = new Statistics; s read arr; s}
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  //TODO
  override def doWrite(): Unit = {
    def writeType(): Unit = {
      FauxquetEncoder writeFieldBegin TYPE_FIELD_DESC
      FauxquetEncoder writeI32 Type.id //this looks different than Parquet, but should have same value
      FauxquetEncoder writeFieldEnd()
    }
    def writeEncodings(): Unit = {
      FauxquetEncoder writeFieldBegin ENCODINGS_FIELD_DESC
      FauxquetEncoder writeListBegin TList(8, encodings.size)

      for (encoding <- encodings) {
        FauxquetEncoder writeI32 encoding.id
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }
    def writePathInSchema(): Unit = {
      FauxquetEncoder writeFieldBegin PATH_IN_SCHEMA_FIELD_DESC
      FauxquetEncoder writeListBegin TList(11, pathInSchema.size)

      for (p <- pathInSchema) {
        FauxquetEncoder writeString p
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }
    def writeCodec(): Unit = {
      FauxquetEncoder writeFieldBegin CODEC_FIELD_DESC
      FauxquetEncoder writeI32 codec.id
      FauxquetEncoder writeFieldEnd()
    }
    def writeNumValues(): Unit = {
      FauxquetEncoder writeFieldBegin NUM_VALUES_FIELD_DESC
      FauxquetEncoder writeI64 numValues
      FauxquetEncoder writeFieldEnd()
    }
    def writeUncompressedSize(): Unit = {
      FauxquetEncoder writeFieldBegin TOTAL_UNCOMPRESSED_SIZE_FIELD_DESC
      FauxquetEncoder writeI64 totalUncompressedSize
      FauxquetEncoder writeFieldEnd()
    }
    def writeCompressedSize(): Unit = {
      FauxquetEncoder writeFieldBegin TOTAL_COMPRESSED_SIZE_FIELD_DESC
      FauxquetEncoder writeI64 totalCompressedSize
      FauxquetEncoder writeFieldEnd()
    }
    def writeKeyValueMetadata(): Unit = {
      FauxquetEncoder writeFieldBegin KEY_VALUE_METADATA_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, keyValueMetadata size)

      for (kv <- keyValueMetadata) {
        kv.write()
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }
    def writeDataPageOffset(): Unit = {
      FauxquetEncoder writeFieldBegin DATA_PAGE_OFFSET_FIELD_DESC
      FauxquetEncoder writeI64 dataPageOffset
      FauxquetEncoder writeFieldEnd()
    }
    def writeIndexPageOffset(): Unit = {
      FauxquetEncoder writeFieldBegin INDEX_PAGE_OFFSET_FIELD_DESC
      FauxquetEncoder writeI64 indexPageOffset
      FauxquetEncoder writeFieldEnd()
    }
    def writeDictionaryPageOffset(): Unit = {
      FauxquetEncoder writeFieldBegin DICTIONARY_PAGE_OFFSET_FIELD_DESC
      FauxquetEncoder writeI64 dictionaryPageOffset
      FauxquetEncoder writeFieldEnd()
    }
    def writeStatistics(): Unit = {
      FauxquetEncoder writeFieldBegin STATISTICS_FIELD_DESC
      statistics.write()
      FauxquetEncoder writeFieldEnd()
    }
    def writeEncodingStats(): Unit = {
      FauxquetEncoder writeFieldBegin ENCODING_STATS_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, encodingStats size)

      for (pes <- encodingStats) {
        pes.write()
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    if (this.Type != null) {
      writeType()
    }

    if (this.encodings != null) {
      writeEncodings()
    }

    if (this.pathInSchema != null) {
      writePathInSchema()
    }

    if (this.codec != null) {
      writeCodec()
    }

    writeNumValues()
    writeUncompressedSize()
    writeCompressedSize()

    if (this.keyValueMetadata != null) {
      writeKeyValueMetadata()
    }

    writeDataPageOffset()

    if (indexPageOffset > -1L) {
      writeIndexPageOffset()
    }

    if (dictionaryPageOffset > -1L) {
      writeDictionaryPageOffset()
    }

    if (statistics != null) {
      writeStatistics()
    }

    if (encodingStats != null) {
      writeEncodingStats()
    }
  }

  override def validate(): Unit = {
    if (numValues == -1L) throw new Error("ColumnMetadata numValues was not found in file.")
    if (totalUncompressedSize == -1L) throw new Error("ColumnMetadata totalUncompressedSize was not found in file.")
    if (totalCompressedSize == -1L) throw new Error("ColumnMetadata totalCompressedSize was not found in file.")
    if (dataPageOffset == -1L) throw new Error("ColumnMetadata dataPageOffset was not found in file.")
  }

  override def className: String = "ColumnMetadata"
}
