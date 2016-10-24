package main.scala.Fauxquet.FauxquetObjs

import main.scala.Fauxquet._

/**
  * Created by james on 8/8/16.
  */
class KeyValue extends Fauxquetable {
  var key: String = _
  var value: String = _

  private val KEY_FIELD_DESC = TField("key", 11, 1)
  private val VALUE_FIELD_DESC = TField("value", 11, 2)

  override def className: String = "KeyValue"

  override def validate(): Unit = if (key == null) throw new Error("Key must not be null in KeyValue")

  override def doMatch(field: TField, arr: SeekableArray[Byte]): Unit = field match {
    case TField(_, 11, x) => x match {
      case 1 => key = FauxquetDecoder readString arr
      case 2 => value = FauxquetDecoder readString arr
      case _ => FauxquetDecoder skip(arr, 11)
    }
    case _ => FauxquetDecoder skip(arr, field Type)
  }

  override def doWrite(): Unit = {
    def writeKey(): Unit = {
      FauxquetEncoder writeFieldBegin KEY_FIELD_DESC
      FauxquetEncoder writeString key
      FauxquetEncoder writeFieldEnd()
    }

    def writeValue(): Unit = {
      FauxquetEncoder writeFieldBegin VALUE_FIELD_DESC
      FauxquetEncoder writeString value
      FauxquetEncoder writeFieldEnd()
    }

    if (this.key != null) {
      writeKey()
    }

    if (this.value != null) {
      writeValue()
    }
  }
}
