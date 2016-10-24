package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class RowGroup extends Fauxquetable {
  var totalByteSize: Long = -1L
  var numRows: Long = -1L

  var columns: List[ColumnChunk] = Nil
  var sortingColumns: List[SortingColumn] = Nil

  private val COLUMNS_FIELD_DESC = TField("columns", 15, 1)
  private val TOTAL_BYTE_SIZE_FIELD_DESC = TField("total_byte_size", 10, 2)
  private val NUM_ROWS_FIELD_DESC = TField("num_rows", 10, 3)
  private val SORTING_COLUMNS_FIELD_DESC = TField("sorting_columns", 15, 4)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 15, x) => x match {
      case 1 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) columns :+= {
          val cc = new ColumnChunk
          cc read arr
          cc
        }
      case 4 =>
        val list = FauxquetDecoder readListBegin arr
        for (i <- 0 until list.size) sortingColumns :+= {
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

  override def doWrite(): Unit = {
    def writeColumns(): Unit = {
      FauxquetEncoder writeFieldBegin COLUMNS_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, columns size)

      for (cc <- columns) {
        cc write()
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    def writeTotalByteSize(): Unit = {
      FauxquetEncoder writeFieldBegin TOTAL_BYTE_SIZE_FIELD_DESC
      FauxquetEncoder writeI64 totalByteSize
      FauxquetEncoder writeFieldEnd()
    }

    def writeNumRows(): Unit = {
      FauxquetEncoder writeFieldBegin NUM_ROWS_FIELD_DESC
      FauxquetEncoder writeI64 numRows
      FauxquetEncoder writeFieldEnd()
    }

    def writeSortingColumns(): Unit = {
      FauxquetEncoder writeFieldBegin SORTING_COLUMNS_FIELD_DESC
      FauxquetEncoder writeListBegin TList(12, sortingColumns size)

      for (sc <- sortingColumns) {
        sc write()
      }

      FauxquetEncoder writeListEnd()
      FauxquetEncoder writeFieldEnd()
    }

    if (this.columns != null) {
      writeColumns()
    }

    writeTotalByteSize()
    writeNumRows()

    if (this.sortingColumns != null) {
      writeSortingColumns()
    }
  }

  override def validate(): Unit = {
    if (totalByteSize == -1L) throw new Error("RowGroup totalByteSize not found in file.")
    if (numRows == -1L) throw new Error("RowGroup numRows not found in file.")
    if (columns == null) throw new Error("RowGroup columns not found in file.")
  }

  override def className: String = "RowGroup"
}
