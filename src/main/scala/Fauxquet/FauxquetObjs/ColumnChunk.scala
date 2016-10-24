package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class ColumnChunk extends Fauxquetable {
  var fileOffset: Long = -1L
  var filePath: String = _

  var metadata: ColumnMetadata = _

  private val FILE_PATH_FIELD_DESC: TField = TField("file_path", 11, 1)
  private val FILE_OFFSET_FIELD_DESC: TField = TField("file_offset", 10, 2)
  private val META_DATA_FIELD_DESC: TField = TField("meta_data", 12, 3)

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

  override def doWrite(): Unit = {
    if (this.filePath != null) {
      FauxquetEncoder writeFieldBegin FILE_PATH_FIELD_DESC
      FauxquetEncoder writeString filePath
      FauxquetEncoder writeFieldEnd()
    }

    FauxquetEncoder writeFieldBegin FILE_OFFSET_FIELD_DESC
    FauxquetEncoder writeI64 fileOffset
    FauxquetEncoder writeFieldEnd()

    if (this.metadata != null) {
      FauxquetEncoder writeFieldBegin META_DATA_FIELD_DESC
      this.metadata.write()
      FauxquetEncoder writeFieldEnd()
    }
  }

  override def validate(): Unit = {
    if (fileOffset == -1L) throw new Error("ColumnChunk fileOffset was not found in file.")
  }

  override def className: String = "ColumnChunk"
}
