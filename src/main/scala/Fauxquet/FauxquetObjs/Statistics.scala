package main.scala.Fauxquet.FauxquetObjs

import java.nio.ByteBuffer

import main.scala.Fauxquet._

/**
  * Created by james on 8/9/16.
  */
class Statistics extends Fauxquetable {
  var max: ByteBuffer = _
  var min: ByteBuffer = _

  var numNull: Long = -1L
  var numDistinct: Long = -1L

  private val MAX_FIELD_DESC = TField("max", 11, 1)
  private val MIN_FIELD_DESC = TField("min", 11, 2)
  private val NULL_COUNT_FIELD_DESC = TField("null_count", 10, 3)
  private val DISTINCT_COUNT_FIELD_DESC = TField("distinct_count", 10, 4)

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 11, x) => x match {
      case 1 => max = FauxquetDecoder readBinary arr
      case 2 => min = FauxquetDecoder readBinary arr
      case _ => FauxquetDecoder skip(arr, 11)
    }
    case TField(_, 10, x) => x match {
      case 3 => numNull = FauxquetDecoder readI64 arr
      case 4 => numDistinct = FauxquetDecoder readI64 arr
      case _ => FauxquetDecoder skip(arr, 10)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeMax(): Unit = {
      FauxquetEncoder writeFieldBegin MAX_FIELD_DESC
      FauxquetEncoder writeBinary max.array()
      FauxquetEncoder writeFieldEnd()
    }

    def writeMin(): Unit = {
      FauxquetEncoder writeFieldBegin MIN_FIELD_DESC
      FauxquetEncoder writeBinary max.array()
      FauxquetEncoder writeFieldEnd()
    }

    def writeNumNull(): Unit = {
      FauxquetEncoder writeFieldBegin NULL_COUNT_FIELD_DESC
      FauxquetEncoder writeI64 numNull
      FauxquetEncoder writeFieldEnd()
    }

    def writeNumDistinct(): Unit = {
      FauxquetEncoder writeFieldBegin DISTINCT_COUNT_FIELD_DESC
      FauxquetEncoder writeI64 numDistinct
      FauxquetEncoder writeFieldEnd()
    }

    if (this.max != null) {
      writeMax()
    }

    if (this.min != null) {
      writeMin()
    }

    if (this.numNull != -1L) {
      writeNumNull()
    }

    if (this.numDistinct != -1L) {
      writeNumDistinct()
    }
  }

  override def validate(): Unit = {}

  override def className: String = "Statistics"
}
