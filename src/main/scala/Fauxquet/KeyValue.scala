package main.scala.Fauxquet

/**
  * Created by james on 8/8/16.
  */
class KeyValue extends Fauxquetable {
  var key: String = _
  var value: String = _

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 11, x) => x match {
      case 1 => key = FauxquetDecoder readString arr
      case 2 => value = FauxquetDecoder readString arr
      case _ => FauxquetDecoder skip(arr, 11)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def write(): Unit = ???

  override def validate(): Unit = if (key == null) throw new Error("Key must not be null in KeyValue")

  override def className: String = "KeyValue"
}
