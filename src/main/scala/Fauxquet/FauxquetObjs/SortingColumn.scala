package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class SortingColumn extends Fauxquetable {
  var columnIndex: Int = -1
  var descending: Option[Boolean] = _
  var nullsFirst: Option[Boolean] = _

  private val COLUMN_IDX_FIELD_DESC = TField("column_idx", 8, 1)
  private val DESCENDING_FIELD_DESC = TField("descending", 2, 2)
  private val NULLS_FIRST_FIELD_DESC = TField("nulls_first", 2, 3)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 8, 1) => columnIndex = FauxquetDecoder readI32 arr
    case TField(_, 2, x) => x match {
      case 2 => descending = Option(FauxquetDecoder readBool arr)
      case 3 => nullsFirst = Option(FauxquetDecoder readBool arr)
      case _ => FauxquetDecoder skip(arr, 2)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeColumnIndex(): Unit = {
      FauxquetEncoder writeFieldBegin COLUMN_IDX_FIELD_DESC
      FauxquetEncoder writeI32 columnIndex
      FauxquetEncoder writeFieldEnd()
    }

    def writeDescending(): Unit = {
      FauxquetEncoder writeFieldBegin DESCENDING_FIELD_DESC
      FauxquetEncoder writeBool descending.get
      FauxquetEncoder writeFieldEnd()
    }

    def writeNullsFirst(): Unit = {
      FauxquetEncoder writeFieldBegin NULLS_FIRST_FIELD_DESC
      FauxquetEncoder writeBool nullsFirst.get
      FauxquetEncoder writeFieldEnd()
    }

    writeColumnIndex()
    writeDescending()
    writeNullsFirst()
  }

  override def validate(): Unit = {
    if (columnIndex == -1) throw new Error("SortingColumn columnIndex not found in file")
    if (descending isEmpty) throw new Error("SortingColumn descending not found in file.")
    if (nullsFirst isEmpty) throw new Error("SortingColumn nullsFirst not found in file.")
  }

  override def className: String = "SortingColumn"
}
