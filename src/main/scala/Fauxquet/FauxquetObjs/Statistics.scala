package main.scala.Fauxquet.FauxquetObjs

import java.nio.ByteBuffer

import main.scala.Fauxquet.{FauxquetDecoder, Fauxquetable, SeekableArray}

/**
  * Created by james on 8/9/16.
  */
class Statistics extends Fauxquetable {
  var max: ByteBuffer = _
  var min: ByteBuffer = _

  var numNull: Long = -1L
  var numDistinct: Long = -1L

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

  //TODO
  override def write(): Unit = ???

  override def validate(): Unit = {}

  override def className: String = "Statistics"
}
