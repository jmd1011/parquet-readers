package main.scala.Fauxquet

/**
  * Created by james on 8/8/16.
  */
class KeyValue extends Fauxquetable {
  var key: String = _
  var value: String = _

  override def read(arr: SeekableArray[Byte]): Unit = {
    var keepGoing: Boolean = true

    FauxquetDecoder readStructBegin()

    while (keepGoing) {
      val field = FauxquetDecoder readFieldBegin arr

      if (field.Type == 0) {
        FauxquetDecoder readStructEnd(field id)
        validate()

        keepGoing = false
      } else field match {
        case TField(_, 11, x) => x match {
          case 1 => key = FauxquetDecoder readString arr
          case 2 => value = FauxquetDecoder readString arr
          case _ => FauxquetDecoder skip(arr, 11)
        }
        case _ => FauxquetDecoder skip(arr, field Type)
      }
    }
  }

  override def write(): Unit = ???

  override def validate(): Unit = if (key == null) throw new Error("Key must not be null in KeyValue")
}
